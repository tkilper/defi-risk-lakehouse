{% macro compute_health_factor(collateral_usd, liq_threshold, debt_usd) %}
    /*
    Inline health factor computation macro.

    Parameters
    ----------
    collateral_usd  : numeric expression — total collateral in USD
    liq_threshold   : numeric expression — weighted liquidation threshold (0–1)
    debt_usd        : numeric expression — total debt in USD

    Returns NULL when debt_usd = 0 to avoid division by zero.
    */
    case
        when {{ debt_usd }} > 0
        then ({{ collateral_usd }} * {{ liq_threshold }}) / {{ debt_usd }}
        else null
    end
{% endmacro %}


{% macro classify_risk_tier(health_factor) %}
    /*
    Map a health factor value to a human-readable risk tier.
    Usage: {{ classify_risk_tier('health_factor') }}
    */
    case
        when {{ health_factor }} is null          then 'NO_DEBT'
        when {{ health_factor }} < 1.00           then 'LIQUIDATABLE'
        when {{ health_factor }} < 1.05           then 'CRITICAL'
        when {{ health_factor }} < 1.10           then 'AT_RISK'
        when {{ health_factor }} < 1.25           then 'WATCH'
        else                                           'HEALTHY'
    end
{% endmacro %}


{% macro shocked_health_factor(collateral_usd, liq_threshold, debt_usd, shock_pct) %}
    /*
    Health factor after applying a collateral price shock.

    Parameters
    ----------
    shock_pct : price drop as a decimal fraction (e.g. 0.20 = 20% drop)
    */
    {{ compute_health_factor(
        collateral_usd ~ ' * (1.0 - ' ~ shock_pct ~ ')',
        liq_threshold,
        debt_usd
    ) }}
{% endmacro %}
