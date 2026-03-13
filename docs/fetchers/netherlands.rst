Netherlands Fetcher
===================

The Netherlands fetcher wraps Rijkswaterstaat WaterWebservices metadata and
observation endpoints for surface-water gauges.

Implemented support includes:

- discharge daily mean and instantaneous
- stage daily mean and instantaneous
- water temperature daily mean and instantaneous

Implementation notes:

- live metadata is sourced from the Rijkswaterstaat catalog
- cached metadata is stored in ``rivretrieve/cached_site_data/netherlands_sites.csv``
- the fetcher prefers the current ``ddapi20`` endpoint family and falls back to the
  legacy WaterWebservices endpoints when needed
- daily series are aggregated from sub-daily observations
- stage values are converted from centimeters to meters
- provider sentinel values are filtered from temperature series before aggregation

.. automodule:: rivretrieve.netherlands
   :members:
