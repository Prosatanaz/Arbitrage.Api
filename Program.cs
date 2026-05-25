using System.Text.Json.Serialization;
using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Application.MarketData.Universe;
using Arbitrage.Api.Infrastructure.HostedServices;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;
using Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;
using Arbitrage.Api.Infrastructure.MarketData.Streams.GateIo;
using Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;
using Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;
using Arbitrage.Api.Infrastructure.MarketData.Universe;
using Arbitrage.Api.Infrastructure.Univerce;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.AddConsole();

// -----------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------

builder.Services.Configure<MarketDataStreamOptions>(
    builder.Configuration.GetSection(MarketDataStreamOptions.SectionName));

builder.Services.Configure<SpreadDetectorOptions>(
    builder.Configuration.GetSection(SpreadDetectorOptions.SectionName));

builder.Services.Configure<DepthTrackingOptions>(
    builder.Configuration.GetSection(DepthTrackingOptions.SectionName));

builder.Services.Configure<ValidatedOpportunityOptions>(
    builder.Configuration.GetSection(ValidatedOpportunityOptions.SectionName));

builder.Services.Configure<TradingPairUniverseOptions>(
    builder.Configuration.GetSection(TradingPairUniverseOptions.SectionName));

// -----------------------------------------------------------------------------
// Trading pair universe discovery / refresh
// -----------------------------------------------------------------------------
//
// This must be registered before hosted market-data workers,
// because StartupTradingPairUniverseRefreshHostedService must refresh
// Data/universe/usdt-perp-pairs.json before streams read it.
// -----------------------------------------------------------------------------

builder.Services.AddHttpClient<BinancePerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });

builder.Services.AddHttpClient<BybitPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });

builder.Services.AddHttpClient<OkxPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });

builder.Services.AddHttpClient<BitgetPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });
builder.Services.AddHttpClient<GateIoPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });
builder.Services.AddHttpClient(
    KuCoinFuturesWebSocketTokenProvider.HttpClientName,
    client =>
    {
        client.BaseAddress = new Uri("https://api-futures.kucoin.com");
        client.Timeout = TimeSpan.FromSeconds(30);
    });
builder.Services.AddHttpClient<KuCoinPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });
builder.Services.AddSingleton<MexcContractMetadataStore>();
builder.Services.AddSingleton<BitMartContractMetadataStore>();

builder.Services.AddHttpClient<BitMartPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<BitMartPerpetualTradingPairDiscoveryClient>());

builder.Services.AddHttpClient<MexcPerpetualTradingPairDiscoveryClient>(
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    });


builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<MexcPerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<KuCoinPerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<KuCoinFuturesWebSocketTokenProvider>();

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<GateIoPerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<BinancePerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<BybitPerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<OkxPerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<ITradingPairDiscoveryClient>(
    serviceProvider =>
        serviceProvider.GetRequiredService<BitgetPerpetualTradingPairDiscoveryClient>());

builder.Services.AddSingleton<TradingPairUniverseRefreshService>();

// -----------------------------------------------------------------------------
// BBO / Spread detection
// -----------------------------------------------------------------------------

builder.Services.AddSingleton<BestBidAskCache>();

builder.Services.AddSingleton<SpreadDetector>();

builder.Services.AddSingleton<LatestSpreadCandidateStore>();

builder.Services.AddSingleton<
    ITradingPairUniverseProvider,
    FileTradingPairUniverseProvider>();

builder.Services.AddSingleton<IBestBidAskStream, BinanceBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, BybitBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, OkxBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, BitgetBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, GateIoBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, KuCoinBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, MexcBboStream>();
builder.Services.AddSingleton<IBestBidAskStream, BitMartBboStream>();

// -----------------------------------------------------------------------------
// Depth tracking / validation
// -----------------------------------------------------------------------------

builder.Services.AddSingleton<DepthSubscriptionTargetStore>();

builder.Services.AddSingleton<OrderBookDepthCache>();

builder.Services.AddSingleton<DepthCandidateEvaluator>();

builder.Services.AddSingleton<IOrderBookDepthStream, BinanceOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, BybitOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, OkxOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, BitgetOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, GateIoOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, KuCoinOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, MexcOrderBookDepthStream>();
builder.Services.AddSingleton<IOrderBookDepthStream, BitMartOrderBookDepthStream>();

// -----------------------------------------------------------------------------
// Validated opportunities
// -----------------------------------------------------------------------------

builder.Services.AddSingleton<LatestValidatedOpportunityStore>();

// -----------------------------------------------------------------------------
// Hosted services
// -----------------------------------------------------------------------------
//
// Order matters.
//
// 1. StartupTradingPairUniverseRefreshHostedService blocks startup and refreshes
//    universe before market-data workers read the file.
// 2. TradingPairUniverseRefreshWorker does periodic refresh later.
// 3. Runtime workers start after universe file is available.
// -----------------------------------------------------------------------------

builder.Services.AddHostedService<StartupTradingPairUniverseRefreshHostedService>();

builder.Services.AddHostedService<TradingPairUniverseRefreshWorker>();

builder.Services.AddHostedService<MarketDataStreamWorker>();
builder.Services.AddHostedService<SpreadDetectionWorker>();
builder.Services.AddHostedService<CandidateDepthSubscriptionWorker>();
builder.Services.AddHostedService<OrderBookDepthStreamWorker>();
builder.Services.AddHostedService<ValidatedOpportunityWorker>();

// -----------------------------------------------------------------------------
// Controllers / Swagger
// -----------------------------------------------------------------------------

builder.Services
    .AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.Converters.Add(
            new JsonStringEnumConverter());
    });

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// -----------------------------------------------------------------------------
// Diagnostics before build
// -----------------------------------------------------------------------------

var bboStreamRegistrations = builder.Services
    .Where(x => x.ServiceType == typeof(IBestBidAskStream))
    .Select(x => x.ImplementationType?.FullName ?? "<unknown>")
    .ToList();

Console.WriteLine("Registered IBestBidAskStream implementations:");

foreach (var registration in bboStreamRegistrations)
{
    Console.WriteLine($" - {registration}");
}

var depthStreamRegistrations = builder.Services
    .Where(x => x.ServiceType == typeof(IOrderBookDepthStream))
    .Select(x => x.ImplementationType?.FullName ?? "<unknown>")
    .ToList();

Console.WriteLine("Registered IOrderBookDepthStream implementations:");

foreach (var registration in depthStreamRegistrations)
{
    Console.WriteLine($" - {registration}");
}

var universeDiscoveryRegistrations = builder.Services
    .Where(x => x.ServiceType == typeof(ITradingPairDiscoveryClient))
    .Select(x => x.ImplementationFactory is not null
        ? "<factory registration>"
        : x.ImplementationType?.FullName ?? "<unknown>")
    .ToList();

Console.WriteLine("Registered ITradingPairDiscoveryClient implementations:");

foreach (var registration in universeDiscoveryRegistrations)
{
    Console.WriteLine($" - {registration}");
}

// -----------------------------------------------------------------------------
// App pipeline
// -----------------------------------------------------------------------------

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// For Docker/http-only runs, keep HTTPS redirect disabled.
// app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();