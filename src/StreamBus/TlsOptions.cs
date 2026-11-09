namespace StreamBus;

/// <summary>TLS settings applied to both the stream protocol and AMQP connections.</summary>
public sealed record StreamBusTlsOptions
{
    /// <summary>Enables TLS for the stream protocol and AMQP connections; false means plain TCP.</summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Server name used for TLS certificate validation (SNI and name matching).
    /// When null, connectors fall back to <see cref="StreamBusOptions.Host"/> at connect time.
    /// </summary>
    public string? ServerName { get; init; }

    /// <summary>Path to a PFX client certificate file; null means no client certificate.</summary>
    public string? ClientCertPath { get; init; }

    /// <summary>Password for the PFX client certificate file; null means no password.</summary>
    public string? ClientCertPassword { get; init; }

    /// <summary>Certificate policy errors accepted despite failing validation; default <see cref="System.Net.Security.SslPolicyErrors.None"/>.</summary>
    public System.Net.Security.SslPolicyErrors AcceptableCertificateErrors { get; init; } = System.Net.Security.SslPolicyErrors.None;

    /// <summary>Allowed TLS protocol versions; the default <see cref="System.Security.Authentication.SslProtocols.None"/> means the operating system default.</summary>
    public System.Security.Authentication.SslProtocols ProtocolVersion { get; init; } = System.Security.Authentication.SslProtocols.None;
}
