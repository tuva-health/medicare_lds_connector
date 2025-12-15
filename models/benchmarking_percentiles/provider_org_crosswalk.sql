WITH pcp_list AS (

    -- universe of PCPs we care about (from attribution, but not using its members)
    SELECT DISTINCT
        provider_id
    FROM {{ ref('provider_attribution__assigned_beneficiaries_current') }}
    WHERE provider_id <> '9999999999'

),

provider_panel AS (

    -- all members who ever saw those PCPs (based on claims)
    SELECT DISTINCT
          provider_id
        , person_id
    FROM (
        SELECT
              mc.billing_id    AS provider_id        -- PCP NPI
            , mc.person_id
        FROM {{ ref('core__medical_claim') }} AS mc
        INNER JOIN pcp_list AS p
            ON p.provider_id = mc.billing_id

        UNION

        SELECT
              mc.rendering_id  AS provider_id        -- PCP NPI
            , mc.person_id
        FROM {{ ref('core__medical_claim') }} AS mc
        INNER JOIN pcp_list AS p
            ON p.provider_id = mc.rendering_id
    )

),

hospital_orgs AS (

    -- hospital org NPIs (same taxonomy criteria as before)
    SELECT
        npi AS hospital_npi
    FROM {{ ref('terminology__provider') }}
    WHERE entity_type_description = 'Organization'
      AND primary_specialty_description IN (
          'Hospital-General'
      )

),

provider_hospital_util AS (

    -- for each PCP, look at all hospital use by *any* member they have ever seen
    SELECT
          pp.provider_id
        , COALESCE(facility_h.hospital_npi, billing_h.hospital_npi) AS hospital_npi
        , COUNT(*)              AS claim_count
        , SUM(mc.paid_amount)   AS total_paid
    FROM {{ ref('core__medical_claim') }} AS mc
    INNER JOIN provider_panel AS pp
        ON mc.person_id = pp.person_id
    LEFT JOIN hospital_orgs AS billing_h
        ON mc.billing_id = billing_h.hospital_npi
    LEFT JOIN hospital_orgs AS facility_h
        ON mc.facility_id = facility_h.hospital_npi
    WHERE billing_h.hospital_npi IS NOT NULL
       OR facility_h.hospital_npi IS NOT NULL
       
    GROUP BY
          pp.provider_id
        , COALESCE(facility_h.hospital_npi, billing_h.hospital_npi)

),

ranked AS (

    SELECT
          provider_id
        , hospital_npi
        , claim_count
        , total_paid
        , ROW_NUMBER() OVER (
            PARTITION BY provider_id
            ORDER BY total_paid DESC
          ) AS rn
    FROM provider_hospital_util

)

SELECT
      r.provider_id
    , r.hospital_npi                     AS attributed_hospital_npi
    , p.provider_organization_name
    , r.claim_count                      AS panel_hospital_claims
    , r.total_paid                       AS panel_hospital_paid
FROM ranked AS r
LEFT JOIN {{ ref('terminology__provider') }} AS p
    ON r.hospital_npi = p.npi
WHERE rn = 1
