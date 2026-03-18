Thailand Fetcher
================

This page documents the Thailand fetcher based on the official public ThaiWater
water-level services.

Official Websites
-----------------

- Main ThaiWater website: https://www.thaiwater.net/
- ThaiWater standards portal: https://standard.thaiwater.net/
- HII telemetry station project background:
  https://www.hii.or.th/en/research-development/project-highlights/2024/02/08/2021-automated-telemetry-station-enhancement-project-to-support-national-water-management/
- Government catalog entry:
  https://gdcatalog.go.th/en/dataset/gdpublish-water-level

Official/Public Endpoints Used
------------------------------

- Station metadata / current snapshot:
  https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_load
- Time series:
  https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_graph
- Example graph request:
  https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_graph?station_type=tele_waterlevel&station_id=1&start_date=2025-01-01&end_date=2025-01-03

Supported Variables
-------------------

- ``constants.STAGE_DAILY_MEAN`` (m)
- ``constants.STAGE_INSTANT`` (m)
- ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
- ``constants.DISCHARGE_INSTANT`` (m³/s)

Implementation Notes
--------------------

- Metadata is built from ``waterlevel_load``.
- Stage and discharge series are retrieved from ``waterlevel_graph``.
- The narrower ``flow`` endpoints are intentionally not used in the first
  Thailand implementation.
- Long requests are internally split into smaller windows because the upstream
  API truncates oversized requests to about the most recent year.
- The live metadata currently populates ``waterlevel_msl`` while
  ``waterlevel_m`` appears mostly null.
- This fetcher therefore interprets ``waterlevel_graph.data.graph_data[].value``
  as stage relative to mean sea level (MSL). This is an implementation
  assumption based on current source behavior and should be revisited if the
  upstream semantics change.
- Discharge is only available for a subset of stations.

Terms Of Use
------------

- See https://www.thaiwater.net/
- See https://standard.thaiwater.net/
- Use of the public endpoints remains subject to provider availability and
  provider terms.

Example Usage
-------------

.. code-block:: python

   from rivretrieve import ThailandFetcher, constants

   fetcher = ThailandFetcher()

   meta = fetcher.get_metadata()
   print(meta.head())

   df = fetcher.get_data(
       gauge_id="505018",
       variable=constants.DISCHARGE_DAILY_MEAN,
       start_date="2020-03-16",
       end_date="2026-03-18",
   )
   print(df.head())

.. automodule:: rivretrieve.thailand
   :members:
