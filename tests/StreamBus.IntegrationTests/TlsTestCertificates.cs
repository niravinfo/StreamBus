using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace StreamBus.IntegrationTests;

/// <summary>
/// Generates the throwaway certificates for the TLS broker fixture: a self-signed
/// test CA and a server certificate for <c>localhost</c> (+ loopback IPs).
/// See <see cref="TlsBroker"/>.
/// </summary>
/// <remarks>
/// The certificates are test fixtures only: they are minted at fixture startup in
/// a temp directory, valid for a short window, and never persisted in the repo.
/// In-process generation keeps the suite cross-platform (no openssl/PowerShell
/// dependency) and gives every run a fresh throwaway CA.
/// </remarks>
internal static class TlsTestCertificates
{
    /// <summary>Certificate file paths handed to the fixture.</summary>
    public sealed record CertificatePaths(string CaCertificate, string ServerCertificate, string ServerKey);

    /// <summary>
    /// Writes <c>ca_certificate.pem</c>, <c>server_certificate.pem</c>, and
    /// <c>server_key.pem</c> (unencrypted PKCS#8) into <paramref name="directory"/>.
    /// </summary>
    public static CertificatePaths Generate(string directory)
    {
        Directory.CreateDirectory(directory);
        // One validity window shared by CA and leaf: independent UtcNow reads
        // can make the leaf outlive its issuer by a few milliseconds, which
        // CertificateRequest.Create rejects.
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = notBefore.AddDays(8);
        using var caKey = RSA.Create(2048);
        using var ca = CreateCertificateAuthority(caKey, notBefore, notAfter);
        using var serverKey = RSA.Create(2048);
        using var server = CreateServerCertificate(ca, serverKey, notBefore, notAfter);

        var paths = new CertificatePaths(
            Path.Combine(directory, "ca_certificate.pem"),
            Path.Combine(directory, "server_certificate.pem"),
            Path.Combine(directory, "server_key.pem"));
        File.WriteAllText(paths.CaCertificate, ca.ExportCertificatePem());
        File.WriteAllText(paths.ServerCertificate, server.ExportCertificatePem());
        File.WriteAllText(paths.ServerKey, WritePem("PRIVATE KEY", serverKey.ExportPkcs8PrivateKey()));
        return paths;
    }

    private static X509Certificate2 CreateCertificateAuthority(RSA key, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        var request = new CertificateRequest(
            "CN=StreamBus Integration Test CA",
            key,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(
            certificateAuthority: true,
            hasPathLengthConstraint: false,
            pathLengthConstraint: 0,
            critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature,
            critical: true));
        return request.CreateSelfSigned(notBefore, notAfter);
    }

    private static X509Certificate2 CreateServerCertificate(X509Certificate2 ca, RSA key, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        var request = new CertificateRequest(
            "CN=localhost",
            key,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        san.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.1", "serverAuth") },
            critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
            critical: true));
        return request.Create(ca, notBefore, notAfter, RandomNumberGenerator.GetBytes(16));
    }

    private static string WritePem(string label, byte[] der)
        => new(PemEncoding.Write(label.AsSpan(), new ReadOnlySpan<byte>(der)));
}
