using System.Text.Json;
using System.Text.Json.Serialization;

namespace Arbitrage.Api.Infrastructure.Execution.Trading;

// Some exchange APIs (notably KuCoin, MEXC) return numeric fields as raw JSON numbers instead of
// the "decimal-as-string" convention used elsewhere (Bybit, OKX, Bitget, HTX). Since exact field
// types weren't verified against live responses for the newer connectors, DTOs use this converter
// on numeric-looking fields so either JSON shape deserializes into the same string representation
// the rest of each client's ParseDecimal/ParseInt helpers already expect.
public sealed class FlexibleNumericStringConverter : JsonConverter<string?>
{
    public override string? Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.Number => reader.TryGetDecimal(out var number)
                ? number.ToString(System.Globalization.CultureInfo.InvariantCulture)
                : reader.GetDouble().ToString(System.Globalization.CultureInfo.InvariantCulture),
            JsonTokenType.Null => null,
            _ => throw new JsonException(
                $"Unexpected token {reader.TokenType} when reading a numeric-or-string field.")
        };
    }

    public override void Write(
        Utf8JsonWriter writer,
        string? value,
        JsonSerializerOptions options)
    {
        if (value is null)
            writer.WriteNullValue();
        else
            writer.WriteStringValue(value);
    }
}
