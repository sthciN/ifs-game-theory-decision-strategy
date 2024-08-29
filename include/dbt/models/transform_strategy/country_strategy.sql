{{ config(materialized='table') }}

{% set decision_strategies = {
        'Stable Currency Policy': [
            "Domestic Currency",
            "US Dollar per Domestic Currency",
            "SDR per Domestic Currency",
            "Exchange Rates",
            "Nominal Effective Exchange Rate",
            "Real Effective Exchange Rate based on Consumer Price Index",
            "Real Effective Exchange Rate based on Unit Labor Costs"
        ],
        'Foreign Exchange Reserves Management': [
            "Foreign Exchange",
            "US Dollar per SDR",
            "SDR per US Dollar",
            "Official Reserve Assets",
            "Other Reserve Assets",
            "Reserve Assets",
            "International Reserves",
            "Total International Reserves"
        ],
        'Gold Standard Approach': [
            "Monetary Gold and SDRs",
            "US Dollars (Gold at Market Price)",
            "US Dollars (gold at 35 SDRs per ounce)",
            "US Dollars per ounce of gold",
            "SDRs (gold at 35 SDRs per ounce)",
            "SDRs per ounce of Gold"
        ],
        'Exchange Rate Stability': [
            "Domestic Currency per ECU",
            "Domestic Currency per Euro",
            "Domestic Currency per SDR",
            "Domestic Currency per U.S. Dollar",
            "US Dollar",
            "US Dollars",
            "Poland Warsaw Stock Exchange WIG-20",
            "Russia RTS Exchange",
            "American Exchange"
        ],
        'Monetary Policy': [
            "Monetary",
            "Monetary Aggregates",
            "Monetary Authorities/Central Bank",
            "Monetary Authorities",
            "Monetary Base",
            "Monetary Base (Euro Area-Wide-Residency)",
            "Monetary Survey"
        ]
    }
%}

SELECT
    i.country_name as country_name,
    CASE
        {% for strategy, indis in decision_strategies.items() %}
            -- {# for indi in indicators #}
                WHEN '{{ indicator_name }}' in indis THEN strategy
            -- {# endfor #}
        {% endfor %}
        ELSE 'unknown'
    END AS strategy
FROM ifs.indicator as i