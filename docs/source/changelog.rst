Changelog
=========

Version 2021.04.0
-----------------

- switch to calver and fsspec pin
- py36 (#462)
- async fixes (#456, 452)

Version 0.6.0
-------------

- update for fsspec 0.9.0 (#448)
- better errors (#443)
- cp to preserve ETAG (#441)
- CI (#435, #427, #395)
- 5GB PUT (#425)
- partial cat (#389)
- direct find (#360)


Version 0.5.0
-------------

- Asynchronous filesystem based on ``aiobotocore``


Version 0.4.0
-------------

- New instances no longer need reconnect (:pr:`244`) by `Martin Durant`_
- Always use multipart uploads when not autocommitting (:pr:`243`) by `Marius van Niekerk`_
- Create ``CONTRIBUTING.md`` (:pr:`248`) by `Jacob Tomlinson`_
- Use autofunction for ``S3Map`` sphinx autosummary (:pr:`251`) by `James Bourbeau`_
- Miscellaneous doc updates (:pr:`252`) by `James Bourbeau`_ 
- Support for Python 3.8 (:pr:`264`) by `Tom Augspurger`_
- Improved performance for ``isdir`` (:pr:`259`) by `Nate Yoder`_
* Increased the minimum required version of fsspec to 0.6.0

.. _`Martin Durant`: https://github.com/martindurant
.. _`Marius van Niekerk`: https://github.com/mariusvniekerk
.. _`Jacob Tomlinson`: https://github.com/jacobtomlinson
.. _`James Bourbeau`: https://github.com/jrbourbeau
.. _`Tom Augspurger`: https://github.com/TomAugspurger
.. _`Nate Yoder`: https://github.com/nateyoder
