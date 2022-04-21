using System;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Transactions;
using Azure.Core.Diagnostics;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using CommandLine;
using Serilog;
using Serilog.Context;
using Serilog.Events;

class Program
{
    [Option] public string? ConnectionString { get; set; } = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");
    [Option] public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(5);
    [Option] public TimeSpan ProcessingDuration { get; set; } = TimeSpan.FromMinutes(17);
    [Option] public int MessageCount { get; set; } = 1;
    [Option] public int ConcurrencyLimit { get; set; } = Environment.ProcessorCount;
    [Option] public string QueueName { get; set; } = "lockrenewal-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    [Option] public TimeSpan MaximumTimeout { get; set; } = TimeSpan.FromHours(2);
    [Option] public bool? UseReceiverPerMessage { get; set; } = false;
    [Option] public bool? UseTransactions { get; set; } = true;

    static Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        return Parser.Default.ParseArguments<Program>(args).WithParsedAsync(o=>o.Main());
    }

    async Task Main()
    {
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(LogEventLevel.Verbose, outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Properties:l} {Message:lj}{NewLine}{Exception}")
            .WriteTo.File($".{QueueName}/.log", rollingInterval: RollingInterval.Hour, retainedFileCountLimit: 24, outputTemplate: "{Timestamp:u} [{Level:u3}] {Properties:l} {Message:lj}{NewLine}{Exception}")
            .CreateLogger();

        using var listener = new AzureEventSourceListener((e, message) =>  Log.Verbose(message), level: EventLevel.Verbose);

        AppDomainLogger.RegisterEvents();

        Log.Information("CommandLine = {0}", Environment.CommandLine);
        Log.Information("Version = {0}", Environment.Version);
        Log.Information("OSVersion = {0}", Environment.OSVersion);
        Log.Information("FrameworkDescription = {0}", RuntimeInformation.FrameworkDescription);
        Log.Information("LatencyMode = {0}", GCSettings.LatencyMode);
        Log.Information("IsServerGC = {0}", GCSettings.IsServerGC);
        Log.Information("LargeObjectHeapCompactionMode = {0}", GCSettings.LargeObjectHeapCompactionMode);
        Log.Information("LockDuration = {0}", LockDuration);
        Log.Information("ProcessingDuration = {0}", ProcessingDuration);
        Log.Information("MessageCount = {0}", MessageCount);
        Log.Information("ConcurrencyLimit = {0}", ConcurrencyLimit);
        Log.Information("MaximumTimeout = {0}", MaximumTimeout);
        Log.Information("QueueName = {0}", QueueName);
        Log.Information("LockDuration = {0}", LockDuration);
        Log.Information("TransactionManager.MaximumTimeout = {0}", TransactionManager.MaximumTimeout);
        Log.Information("UseReceiverPerMessage = {0}", UseReceiverPerMessage);
        Log.Information("UseTransactions = {0}", UseTransactions);

        TransactionManagerHelper.TryConfigureTransactionTimeout(MaximumTimeout);

        if (TransactionManager.MaximumTimeout < ProcessingDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(ProcessingDuration), ProcessingDuration, "Cannot be larger than TransactionManager.MaximumTimeout");
        }

        var man = new ServiceBusAdministrationClient(ConnectionString);

        var exitCts = new CancellationTokenSource();
        var exit = exitCts.Token;

        Console.CancelKeyPress += (_, ea) =>
        {
            Log.Information("Quiting...");
            ea.Cancel = true;
            exitCts.Cancel();
        };

        Console.WriteLine("Press CTRL+C to gracefully exit and cleanup queue.");

        using var limiter = new SemaphoreSlim(ConcurrencyLimit);

        await man.CreateQueueAsync(new CreateQueueOptions(QueueName) { LockDuration = LockDuration, MaxDeliveryCount = 100, AutoDeleteOnIdle = TimeSpan.FromMinutes(10) });

        var asbOptions = new ServiceBusClientOptions
        {
            EnableCrossEntityTransactions = UseTransactions!.Value,
            // RetryOptions
        };

        var receiveOptions = new ServiceBusReceiverOptions
        {
            PrefetchCount = 0,
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        };

        await using var client = new ServiceBusClient(ConnectionString, asbOptions);

        Log.Information("PURGE");

        await using (var purgeReceiver = client.CreateReceiver(QueueName, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete }))
        {
            ServiceBusReceivedMessage purgeMessage;
            while (null != (purgeMessage = await purgeReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), exit)))
            {
                Log.Information("Purging {messageId}", purgeMessage.MessageId);
            }
        }

        await using var receiver = client.CreateReceiver(QueueName, receiveOptions);
        await using var sender = client.CreateSender(QueueName);

        try
        {
            Log.Information("SEED");

            long messageIdCount = 0;

            var ticks = ProcessingDuration.Ticks;

            var seedingTasks = new List<Task>(MessageCount);

            List<long> durations = new List<long>(MessageCount);
            for (int i = 1; i <= MessageCount; i++)
            {
                var duration = i * ticks / MessageCount;
                durations.Add(duration);
            }

            durations.Shuffle();

            seedingTasks.AddRange(durations.Select(d => sender.SendMessageAsync(new ServiceBusMessage(d.ToString()) { MessageId = $"{++messageIdCount:0000}" }, exit)));

            await Task.WhenAll(seedingTasks);

            Log.Information("PROCESS");

            while (!exit.IsCancellationRequested)
            {
                await limiter.WaitAsync(exit);

                Process();

                async void Process() // Intentionally void, fire and forget
                {
                    try
                    {
                        Log.Verbose("ReceiveMessageAsync");

                        // Prevents FirstChanceException AppDomain event
                        var receiveTask = receiver.ReceiveMessageAsync(TimeSpan.FromMinutes(4), exit).ContinueWith(t => t, TaskContinuationOptions.ExecuteSynchronously);

                        if (receiveTask.IsCanceled) return;

                        var inMsg = receiveTask.Result.Result;

                        if (inMsg == null)
                        {
                            Log.Verbose("No result");
                            return;
                        }

                        using var l = LogContext.PushProperty("Id", inMsg.MessageId);

                        var processingDuration = TimeSpan.FromTicks(long.Parse(inMsg.Body.ToString()));

                        Log.Information("#Delivery:{0:000} Delay:{1:mm\\:ss}", inMsg.DeliveryCount, processingDuration);

                        using var doneCts = CancellationTokenSource.CreateLinkedTokenSource(exit);
                        doneCts.CancelAfter(processingDuration);

                        var renewalReceiver = UseReceiverPerMessage!.Value
                            ? client.CreateReceiver(QueueName)
                            : receiver;

                        while (!doneCts.IsCancellationRequested)
                        {
                            var remaining = inMsg.LockedUntil - DateTimeOffset.UtcNow;
                            var buffer = TimeSpan.FromTicks(Math.Min(remaining.Ticks / 2, TimeSpan.FromSeconds(10).Ticks));
                            remaining -= buffer;

                            Log.Debug("Locked until {0:T}, renewal in {1:N}s", inMsg.LockedUntil, remaining.TotalSeconds);

                            if (remaining.Ticks > 1)
                            {
                                // Prevents FirstChanceException AppDomain event
                                var task = await Task.Delay(remaining, doneCts.Token).ContinueWith(t => t, TaskContinuationOptions.ExecuteSynchronously); 
                                if (task.IsCanceled) break;
                            }

                            await renewalReceiver.RenewMessageLockAsync(inMsg, exit);
                        }

                        using var scope = UseTransactions.Value ? new TransactionScope(TransactionScopeAsyncFlowOption.Enabled) : null;

                        try
                        {
                            Log.Debug("SendMessageAsync");

                            // Clone incoming message
                            var outMsg = new ServiceBusMessage(inMsg.Body) { MessageId = $"{++messageIdCount:0000}" };
                            await sender.SendMessageAsync(outMsg, exit);

                            Log.Debug("Complete/Commit");
                            await receiver.CompleteMessageAsync(inMsg, exit);

                            scope?.Complete();
                        }
                        catch (ServiceBusException sbe) when (sbe.Reason == ServiceBusFailureReason.MessageLockLost)
                        {
                            Log.Error("MessageLockLost"); // No need to invoke AbandonMessageAsync as lock already lost
                        }
                        catch (Exception e)
                        {
                            Log.Error(e, "Process message failed");
                            await receiver.AbandonMessageAsync(inMsg, null, default); //Try to abandon even while crashing to release message ASAP
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Log.Warning("Process cancelled");
                    }
                    catch (Exception e)
                    {
                        Log.Error(e, "Process failed");
                    }
                    finally
                    {
                        limiter.Release();
                    }
                }
            }
            while (limiter.CurrentCount != ConcurrencyLimit)
            {
                Log.Information("waiting for {0} processor(s) to finish...", ConcurrencyLimit - limiter.CurrentCount);
                await Task.Delay(100);
            }
        }
        catch (OperationCanceledException)
        {
            while (limiter.CurrentCount != ConcurrencyLimit)
            {
                Log.Information("waiting for {0} processor(s) to finish...", ConcurrencyLimit - limiter.CurrentCount);
                await Task.Delay(100);
            }
        }
        catch (Exception e) // when (e is not OperationCanceledException)
        {
            Log.Fatal(e,"Main");
        }
        finally
        {
            Log.Information("DeleteQueueAsync");
            await man.DeleteQueueAsync(QueueName);
            Log.Information("Done!");
        }
    }
}

static class TransactionManagerHelper
{
    public static bool TryConfigureTransactionTimeout(TimeSpan timeout)
    {
        try
        {
#if NETFRAMEWORK
            SetTransactionManagerField("_cachedMaxTimeout", true);
            SetTransactionManagerField("_maximumTimeout", timeout);
#else
            SetTransactionManagerField("s_cachedMaxTimeout", true);
            SetTransactionManagerField("s_maximumTimeout", timeout);
#endif
            return true;
        }
        catch
        {
            return false;
        }

        void SetTransactionManagerField(string fieldName, object value) =>
            typeof(TransactionManager)
                .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static)!
                .SetValue(null, value);
    }
}

static class ListExtensions
{
    static Random rng = new Random();

    public static void Shuffle<T>(this IList<T> list)
    {
        lock (rng)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                var k = rng.Next(n + 1);
                (list[k], list[n]) = (list[n], list[k]);
            }
        }
    }
}

class AppDomainLogger
{
    static readonly HashSet<Guid> ids = new HashSet<Guid>();

    public static void RegisterEvents(bool captureStackTrace = false)
    {
        var appDomain = AppDomain.CurrentDomain;
        appDomain.UnhandledException += (sender, ea) => Log.Fatal((Exception)ea.ExceptionObject, "UnhandledException");
        appDomain.FirstChanceException += (sender, ea) =>
        {
            var ex = ea.Exception;
            if(captureStackTrace) SetStackTrace(ex, new StackTrace(1));
            var value = ex.ToString();
            var stackTraceId = GuidUtility.Create(Guid.Empty, value);

            lock (ids)
            {
                if (ids.Add(stackTraceId))
                {
                    Log.Verbose(ex, "Stacktrace {stackTraceId} {type} {message}", stackTraceId, ex.GetType(), ex.Message);
                }
                else
                {
                    Log.Verbose("FirstChanceException {stackTraceId} {type} {message}", stackTraceId, ex.GetType(), ex.Message);
                }
            }
        };
    }

    static readonly Func<Exception, StackTrace, Exception> SetStackTrace = new Func<Func<Exception, StackTrace, Exception>>(() =>
    {
        var target = Expression.Parameter(typeof(Exception));
        var stack = Expression.Parameter(typeof(StackTrace));
        var traceFormatType = typeof(StackTrace).GetNestedType("TraceFormat", BindingFlags.NonPublic)!;
        var toString = typeof(StackTrace).GetMethod("ToString", BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { traceFormatType }, null)!;
        var normalTraceFormat = Enum.GetValues(traceFormatType).GetValue(0);
        var stackTraceString = Expression.Call(stack, toString, Expression.Constant(normalTraceFormat, traceFormatType));
        var stackTraceStringField = typeof(Exception).GetField("_stackTraceString", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var assign = Expression.Assign(Expression.Field(target, stackTraceStringField), stackTraceString);
        return Expression.Lambda<Func<Exception, StackTrace, Exception>>(Expression.Block(assign, target), target, stack).Compile();
    })();
}

class InputHelper
{
    public static string Ask(string message, string defaultValue)
    {
        Console.Write($"{message} [{defaultValue}]:");
        var line = Console.ReadLine();
        if (string.IsNullOrEmpty(line)) return defaultValue;
        return line;
    }
}
