with countries as (
    SELECT
        -- Top level keys
        country.value:names.common::STRING AS country_name,
        country.value:names.official::STRING AS official_name,
        country.value:capitals[0]:name::STRING AS capital_city,
        country.value:calling_codes[0]::string AS calling_codes,

        country.value:government_type::STRING AS government_type,
        country.value:population::NUMBER AS population,
        country.value:region::STRING AS region,
        country.value:subregion::STRING AS subregion,

        -- Array keys (getting the first element)
        country.value:continents[0]::STRING AS continent,
        country.value:classification.sovereign::STRING AS independent,
        country.value:area.kilometers::NUMBER AS kilometers,
        country.value:area.miles::NUMBER AS miles,
        country.value:landlocked::STRING AS landlocked,
        country.value:memberships.un::BOOLEAN AS un_member,
        country.value:memberships.eu::BOOLEAN AS eu_member,
        country.value:memberships.arab_league::BOOLEAN AS arab_league_member,
        country.value:memberships.african_union::BOOLEAN AS african_union_member,

        country.value:date.start_of_week::STRING AS start_of_week,
        country.value:timezones[0]::STRING AS timezone,

        -- Dynamic Currency keys
        currency.value:code::STRING AS currency_code,
        currency.value:name::STRING AS currency_name,
        currency.value:symbol::STRING AS currency_symbol

    FROM {{ ref('stg_triplens_countries') }} AS stg
        JOIN LATERAL FLATTEN(input => stg.PAYLOAD) country
        JOIN LATERAL FLATTEN(input => country.value:currencies) currency
)

select * from countries