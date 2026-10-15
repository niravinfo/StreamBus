using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace StreamBus;

public interface IStreamRegistrationContext
{
    IServiceCollection Services { get; }
}

public interface IHostConfigurator
{
    void Credentials(string user, string password);
    void StreamPort(int port = 5552);
    void AmqpPort(int port = 5672);
    void Heartbeat(TimeSpan heartbeat);
    void ProducersPerConnection(int count = 1);
    void ConsumersPerConnection(int count = 1);
}

public interface IStreamBusConfigurator
{
    void Host(string host, string vhost, Action<IHostConfigurator> configure);
    void Serializer(IStreamSerializer serializer);
    void Serializer(Action<JsonSerializerOptions> configure);
    void DeclareStream(string name, Action<StreamDeclarationKnobsBuilder>? configure = null);
    void DeclareSuperStream(string name, int partitions, Action<StreamDeclarationKnobsBuilder>? configure = null);

    /// <summary>
    /// Temporary per-bus hosting carry-over. Task 7 replaces this with global AddStreamBusHosting.
    /// </summary>
    void Hosting(Action<StreamBusHostOptionsBuilder> configure);
}

internal sealed class StreamRegistrationContext : IStreamRegistrationContext
{
    public StreamRegistrationContext(IServiceCollection services) => Services = services;
    public IServiceCollection Services { get; }
}

internal sealed class StreamBusConfigurator : IStreamBusConfigurator
{
    private string? _host;
    private string? _vhost;
    private string _user = "";
    private string _password = "";
    private int _streamPort = 5552;
    private int _amqpPort = 5672;
    private TimeSpan _heartbeat = TimeSpan.FromSeconds(60);
    private int _producers = 1;
    private int _consumers = 1;
    private IStreamSerializer _serializer = SystemTextJsonStreamSerializer.Default;
    private readonly List<StreamDeclaration> _streams = new();
    private readonly List<SuperStreamDeclaration> _supers = new();
    private StreamBusHostOptions _hosting = new();
    private bool _hostCalled;

    internal void ApplySectionVhost(IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(section);
        var v = section.GetSection("Vhost").Value;
        if (!string.IsNullOrWhiteSpace(v))
            _vhost = v.Trim();
    }

    public void Host(string host, string vhost, Action<IHostConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(vhost);
        ArgumentNullException.ThrowIfNull(configure);
        if (_hostCalled)
            throw new StreamBusConfigurationException("Host may be called exactly once per bus.");
        _host = host.Trim();
        _vhost = vhost.Trim();
        var inner = new HostConfigurator(this);
        configure(inner);
        _hostCalled = true;
    }

    public void Serializer(IStreamSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
    }

    public void Serializer(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new JsonSerializerOptions();
        configure(options);
        _serializer = new SystemTextJsonStreamSerializer(options);
    }

    public void DeclareStream(string name, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        var declaration = knobs.BuildStream(name.Trim());
        TopologyValidation.ValidateStream(declaration);
        _streams.Add(declaration);
    }

    public void DeclareSuperStream(string name, int partitions, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        var declaration = knobs.BuildSuper(name.Trim(), partitions);
        TopologyValidation.ValidateSuper(declaration);
        _supers.Add(declaration);
    }

    public void Hosting(Action<StreamBusHostOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new StreamBusHostOptionsBuilder();
        configure(builder);
        _hosting = builder.Build();
    }

    internal StreamBusOptions Build(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        if (!_hostCalled)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host must be configured exactly once.");
        if (string.IsNullOrWhiteSpace(_host))
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host host must be supplied.");
        if (string.IsNullOrWhiteSpace(_vhost))
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host vhost must be supplied.");
        if (_streamPort is < 1 or > 65535)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: StreamPort must be in 1..65535.");
        if (_amqpPort is < 1 or > 65535)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: AmqpPort must be in 1..65535.");
        if (_heartbeat <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Heartbeat must be positive.");
        if (_producers < 1 || _producers > byte.MaxValue)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: ProducersPerConnection must be in 1..{byte.MaxValue}.");
        if (_consumers < 1 || _consumers > byte.MaxValue)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: ConsumersPerConnection must be in 1..{byte.MaxValue}.");

        var plainNames = new HashSet<string>(_streams.Select(s => s.Name), StringComparer.Ordinal);
        foreach (var s in _supers)
        {
            if (plainNames.Contains(s.Name))
                throw new StreamConfigurationConflictException(s.Name, $"super stream '{s.Name}'", $"plain stream '{s.Name}'");
        }

        var options = new StreamBusOptions
        {
            Vhost = _vhost.Trim(),
            UserName = _user,
            Password = _password,
            Host = _host.Trim(),
            StreamPort = _streamPort,
            AmqpPort = _amqpPort,
            Heartbeat = _heartbeat,
            Connection = new StreamBusConnectionOptions
            {
                ProducersPerConnection = _producers,
                ConsumersPerConnection = _consumers,
            },
            Hosting = _hosting,
            Topology = new StreamTopologyDeclarations
            {
                Streams = _streams.ToList(),
                SuperStreams = _supers.ToList(),
            },
            Serializer = _serializer,
        };
        StreamBusCredentialValidator.Validate(options);
        return options;
    }

    private sealed class HostConfigurator : IHostConfigurator
    {
        private readonly StreamBusConfigurator _owner;
        public HostConfigurator(StreamBusConfigurator owner) => _owner = owner;

        public void Credentials(string user, string password)
        {
            ArgumentNullException.ThrowIfNull(user);
            ArgumentNullException.ThrowIfNull(password);
            _owner._user = user;
            _owner._password = password;
        }

        public void StreamPort(int port = 5552)
        {
            if (port is < 1 or > 65535)
                throw new StreamBusConfigurationException("StreamPort must be in 1..65535.");
            _owner._streamPort = port;
        }

        public void AmqpPort(int port = 5672)
        {
            if (port is < 1 or > 65535)
                throw new StreamBusConfigurationException("AmqpPort must be in 1..65535.");
            _owner._amqpPort = port;
        }

        public void Heartbeat(TimeSpan heartbeat)
        {
            if (heartbeat <= TimeSpan.Zero)
                throw new StreamBusConfigurationException("Heartbeat must be positive.");
            _owner._heartbeat = heartbeat;
        }

        public void ProducersPerConnection(int count = 1)
        {
            if (count < 1 || count > byte.MaxValue)
                throw new StreamBusConfigurationException($"ProducersPerConnection must be in 1..{byte.MaxValue}.");
            _owner._producers = count;
        }

        public void ConsumersPerConnection(int count = 1)
        {
            if (count < 1 || count > byte.MaxValue)
                throw new StreamBusConfigurationException($"ConsumersPerConnection must be in 1..{byte.MaxValue}.");
            _owner._consumers = count;
        }
    }
}

public static class StreamBusConfiguratorExtensions
{
    public static IServiceCollection AddStreamBus(
        this IServiceCollection services,
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, typeof(DefaultBus), section: null, configure);
    }

    public static IServiceCollection AddStreamBus<TBus>(
        this IServiceCollection services,
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, typeof(TBus), section: null, configure);
    }

    public static IServiceCollection AddStreamBus(
        this IServiceCollection services,
        IConfigurationSection section,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        return AddCore(services, typeof(DefaultBus), section, configure);
    }

    public static IServiceCollection AddStreamBus<TBus>(
        this IServiceCollection services,
        IConfigurationSection section,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        return AddCore(services, typeof(TBus), section, configure);
    }

    private static IServiceCollection AddCore(
        IServiceCollection services,
        Type busType,
        IConfigurationSection? section,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure)
    {
        var configurator = new StreamBusConfigurator();
        if (section is not null)
            configurator.ApplySectionVhost(section);
        var context = new StreamRegistrationContext(services);
        configure?.Invoke(context, configurator);
        var options = configurator.Build(busType);
        ServiceCollectionExtensions.AddShared(services, busType, options);
        if (busType == typeof(DefaultBus))
            services.TryAddSingleton<IStreamBus>(sp => sp.GetRequiredService<IStreamBusRegistry>().GetOrCreateBus(options.Vhost));
        else
            services.TryAddSingleton(typeof(IStreamBus<>).MakeGenericType(busType), sp => ((IStreamBusRegistry)sp.GetRequiredService<IStreamBusRegistry>()).GetOrCreateEntry(busType, options.Vhost).Bus);
        return services;
    }
}
