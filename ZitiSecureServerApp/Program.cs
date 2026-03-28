using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MSLogLevel = Microsoft.Extensions.Logging.LogLevel;
using OpenZiti;
using OpenZiti.Debugging;

class Program
{
    private const string ServiceName = "my-service";
    private const string IdentityFile = "server1.json";

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern void Ziti_lib_init();

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern int Ziti_load_context(out IntPtr ztx, string identityFile);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern IntPtr Ziti_socket(int type);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern int Ziti_bind(IntPtr sock, IntPtr ztx, string service, string? terminator);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern int Ziti_listen(IntPtr sock, int backlog);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern IntPtr Ziti_accept(IntPtr sock, byte[] caller, int callerLen);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern int Ziti_close(IntPtr sock);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern void Ziti_lib_shutdown();

    static async Task Main(string[] args)
    {
        API.SetLogLevel((MSLogLevel)ZitiLogLevel.INFO);

        using var loggerFactory = LoggerFactory.Create(b =>
            b.AddConsole().SetMinimumLevel(MSLogLevel.Debug));
        var logger = loggerFactory.CreateLogger<Program>();

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            logger.LogInformation("Shutdown signal received...");
            cts.Cancel();
        };

        var identityPath = Path.Combine(AppContext.BaseDirectory, IdentityFile);
        if (!File.Exists(identityPath))
        {
            logger.LogCritical("Identity file not found: {Path}", identityPath);
            Environment.Exit(1);
        }

        // ── Init ONCE — never inside the loop ────────────────────────────────
        Ziti_lib_init();

        int rc = Ziti_load_context(out IntPtr ztx, identityPath);
        if (rc != 0)
        {
            logger.LogCritical("Ziti_load_context failed: {Code}", rc);
            Environment.Exit(1);
        }
        // ─────────────────────────────────────────────────────────────────────

        int restartDelay = 5;
        int attempt = 0;

        while (!cts.Token.IsCancellationRequested)
        {
            attempt++;
            logger.LogInformation("Server starting (attempt #{Attempt})...", attempt);

            IntPtr srv = IntPtr.Zero;
            try
            {
                // Only socket/bind/listen restart on each attempt
                srv = Ziti_socket(1);
                if (srv == IntPtr.Zero || srv == new IntPtr(-1))
                    throw new Exception("Ziti_socket failed");

                rc = Ziti_bind(srv, ztx, ServiceName, null);
                if (rc != 0)
                    throw new Exception($"Ziti_bind failed: {rc}");

                rc = Ziti_listen(srv, 10);
                if (rc != 0)
                    throw new Exception($"Ziti_listen failed: {rc}");

                logger.LogInformation(
                    "Listening securely on Ziti service '{Service}'...", ServiceName);

                restartDelay = 5;

                while (!cts.Token.IsCancellationRequested)
                {
                    IntPtr clientHandle;
                    string caller;
                    try
                    {
                        (clientHandle, caller) = await Task.Run(() =>
                        {
                            var buf = new byte[128];
                            IntPtr clt = Ziti_accept(srv, buf, buf.Length);
                            string id = System.Text.Encoding.UTF8
                                            .GetString(buf).TrimEnd('\0');
                            return (clt, id);
                        }, cts.Token);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Accept error — restarting listener...");
                        break;
                    }

                    if (clientHandle == IntPtr.Zero || clientHandle == new IntPtr(-1))
                    {
                        logger.LogWarning("Accept returned invalid handle, skipping...");
                        continue;
                    }

                    _ = HandleClientAsync(clientHandle, caller, logger, cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Cancelled — not restarting.");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Error on attempt #{Attempt}. Restarting in {Delay}s...",
                    attempt, restartDelay);
            }
            finally
            {
                if (srv != IntPtr.Zero && srv != new IntPtr(-1))
                {
                    Ziti_close(srv);
                    srv = IntPtr.Zero;
                }
            }

            if (!cts.Token.IsCancellationRequested)
            {
                logger.LogInformation(
                    "Restarting in {Delay}s... (Ctrl+C to exit)", restartDelay);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(restartDelay), cts.Token);
                }
                catch (OperationCanceledException) { break; }

                restartDelay = Math.Min(restartDelay * 2, 60);
            }
        }

        Ziti_lib_shutdown();
        logger.LogInformation("Server shut down cleanly.");
    }

    static async Task HandleClientAsync(
        IntPtr clientHandle,
        string caller,
        ILogger logger,
        CancellationToken ct)
    {
        try
        {
            logger.LogInformation("Client connected: {Caller}", caller);

            var socket = new Socket(
                new SafeSocketHandle(clientHandle, ownsHandle: true));

            using (socket)
            using (var stream = new NetworkStream(socket, ownsSocket: true))
            {
                var message = "Hello from secure server!"u8.ToArray();
                await stream.WriteAsync(message, ct);
                await stream.FlushAsync(ct);
                logger.LogInformation("Response sent to: {Caller}", caller);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Connection cancelled: {Caller}", caller);
        }
        catch (IOException ex)
        {
            logger.LogError(ex, "I/O error: {Caller}", caller);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error handling client: {Caller}", caller);
        }
    }
}