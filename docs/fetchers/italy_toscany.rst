Italy Toscany Fetcher
=====================

The Italy-Toscany fetcher wraps the public hydrological services operated by the
Regione Toscana SIR platform for idrometric stations.

Implemented support includes:

- discharge daily mean
- stage daily mean

Implementation notes:

- live metadata merges the public ``geo:cf_idrometri`` WFS layer with the public
  SIR monitoring station table so river names, basin labels, and stable station
  coordinates are retained together
- cached metadata is stored in ``rivretrieve/cached_site_data/italy_toscany_sites.csv``
- station coordinates are transformed from EPSG:3003 to WGS84
- daily series are downloaded from the SIR archive endpoint using
  ``IDST=idro_p`` for discharge and ``IDST=idro_l`` for stage
- archive CSVs use semicolon separators, decimal commas, Latin-1 text, and a
  separate quality-flag column; missing values are removed during parsing
- discharge availability varies by station, so ``get_data()`` may return an
  empty DataFrame for stations without archived flow data

.. automodule:: rivretrieve.italy_toscany
   :members:
