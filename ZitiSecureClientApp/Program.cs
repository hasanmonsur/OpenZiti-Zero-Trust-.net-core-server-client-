using Microsoft.Extensions.Logging;
using OpenZiti;
using OpenZiti.Debugging;
using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;
using MSLogLevel = Microsoft.Extensions.Logging.LogLevel;

class Program
{
    private const string ServiceName = "my-service";
    private const string IdentityFile = "client1.json";

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern void Ziti_lib_init();

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern int Ziti_load_context(out IntPtr ztx, string identityFile);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern IntPtr Ziti_socket(int type);

    [DllImport("ziti4dotnet", CallingConvention = CallingConvention.Cdecl)]
    static extern int Ziti_connect(IntPtr sock, IntPtr ztx, string service, string? terminator);

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
            logger.LogInformation("Connecting (attempt #{Attempt})...", attempt);

            IntPtr sock = IntPtr.Zero;
            try
            {
                // Only socket/connect restarts on each attempt
                sock = Ziti_socket(1);
                if (sock == IntPtr.Zero || sock == new IntPtr(-1))
                    throw new Exception("Ziti_socket failed");

                rc = await Task.Run(() =>
                    Ziti_connect(sock, ztx, ServiceName, null), cts.Token);
                if (rc != 0)
                    throw new Exception($"Ziti_connect failed: {rc}");

                logger.LogInformation(
                    "Connected securely to Ziti service '{Service}'", ServiceName);

                restartDelay = 5;

                var socket = new Socket(
                    new SafeSocketHandle(sock, ownsHandle: true));
                sock = IntPtr.Zero;

                using (socket)
                using (var stream = new NetworkStream(socket, ownsSocket: true))
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        // Read server greeting first
                        var buffer = new byte[4096];
                        int bytesRead;
                        try
                        {
                            bytesRead = await stream.ReadAsync(buffer, cts.Token);
                        }
                        catch (OperationCanceledException) { break; }

                        if (bytesRead == 0)
                        {
                            logger.LogInformation("Server closed — reconnecting...");
                            break;
                        }

                        var response = System.Text.Encoding.UTF8
                                           .GetString(buffer, 0, bytesRead);
                        logger.LogInformation("Received: {Response}", response);

                        // Send ACK (best-effort)
                        try
                        {
                            var ack = System.Text.Encoding.UTF8.GetBytes(
                                $"ACK from client at {DateTime.UtcNow:O}");
                            await stream.WriteAsync(ack, cts.Token);
                            await stream.FlushAsync(cts.Token);
                            logger.LogInformation("Sent ACK to server");
                        }
                        catch (IOException)
                        {
                            logger.LogInformation("Server closed after greeting — normal.");
                        }

                        try
                        {
                            await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);
                        }
                        catch (OperationCanceledException) { break; }

                        break; // reconnect for next message
                    }
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Cancelled — not reconnecting.");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Error on attempt #{Attempt}. Reconnecting in {Delay}s...",
                    attempt, restartDelay);
            }
            finally
            {
                if (sock != IntPtr.Zero && sock != new IntPtr(-1))
                {
                    Ziti_close(sock);
                    sock = IntPtr.Zero;
                }
            }

            if (!cts.Token.IsCancellationRequested)
            {
                logger.LogInformation(
                    "Reconnecting in {Delay}s... (Ctrl+C to exit)", restartDelay);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(restartDelay), cts.Token);
                }
                catch (OperationCanceledException) { break; }

                restartDelay = Math.Min(restartDelay * 2, 60);
            }
        }

        Ziti_lib_shutdown();
        logger.LogInformation("Client shut down cleanly.");
    }

}