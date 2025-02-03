# Changelog

## [0.8.3](https://github.com/Autodesk/pgbelt/compare/v0.8.2...v0.8.3) (2025-02-03)


### Bug Fixes

* loggers were overwriting themselves on disk. Separated now [#673](https://github.com/Autodesk/pgbelt/issues/673) ([#674](https://github.com/Autodesk/pgbelt/issues/674)) ([f39c73d](https://github.com/Autodesk/pgbelt/commit/f39c73d5932604300d6859cdcc94562b86a4fc66))


### Documentation

* various updates ([#676](https://github.com/Autodesk/pgbelt/issues/676)) ([64f62dd](https://github.com/Autodesk/pgbelt/commit/64f62dd0d33cdb248e1d57d3e4f66cb307962fa2))

## [0.8.2](https://github.com/Autodesk/pgbelt/compare/v0.8.1...v0.8.2) (2025-01-06)


### Bug Fixes

* support for indexes with quotes in the name [#652](https://github.com/Autodesk/pgbelt/issues/652) ([#653](https://github.com/Autodesk/pgbelt/issues/653)) ([1b7d90c](https://github.com/Autodesk/pgbelt/commit/1b7d90cd13ca2e03d46a8f3c5b95599b35a8cac9))

## [0.8.1](https://github.com/Autodesk/pgbelt/compare/v0.8.0...v0.8.1) (2024-11-26)


### Bug Fixes

* remove dump-dst-indexes, useless and add 0 statement timeout to creating indexes ([#629](https://github.com/Autodesk/pgbelt/issues/629)) ([06285e9](https://github.com/Autodesk/pgbelt/commit/06285e9da801ac184ab54dee4ae3c49d61e49801))
* remove useless command (dump_constraints for dst dump...) ([#625](https://github.com/Autodesk/pgbelt/issues/625)) ([8a1f372](https://github.com/Autodesk/pgbelt/commit/8a1f3720d80f28742e9b628172551e5f297b24fb))
* remove-constraints errors if no NOT VALIDS involved ([#626](https://github.com/Autodesk/pgbelt/issues/626)) ([e3fc113](https://github.com/Autodesk/pgbelt/commit/e3fc11367fac8d4c0c15310a970cf35fe67e2cde))
* remove-indexes syntax error fix ([#630](https://github.com/Autodesk/pgbelt/issues/630)) ([4f779f9](https://github.com/Autodesk/pgbelt/commit/4f779f93d6fe4c39ce81bfb6f82e6767a174c963))

## [0.8.0](https://github.com/Autodesk/pgbelt/compare/v0.7.11...v0.8.0) (2024-11-19)


### Features

* run ANALYZE as part of create-indexes ([#612](https://github.com/Autodesk/pgbelt/issues/612)) ([29294ed](https://github.com/Autodesk/pgbelt/commit/29294ed9c4325ea0b5c699eda2398d4b46735e06))
* run ANALYZE as part of create-indexes [#611](https://github.com/Autodesk/pgbelt/issues/611) ([29294ed](https://github.com/Autodesk/pgbelt/commit/29294ed9c4325ea0b5c699eda2398d4b46735e06))


### Bug Fixes

* hide progress when reverse replication is running [#608](https://github.com/Autodesk/pgbelt/issues/608) ([#610](https://github.com/Autodesk/pgbelt/issues/610)) ([8f143a8](https://github.com/Autodesk/pgbelt/commit/8f143a868126ee613b0607659747f14f95b5fd17))
* run analyze without statement timeout ([#606](https://github.com/Autodesk/pgbelt/issues/606)) ([3bac10c](https://github.com/Autodesk/pgbelt/commit/3bac10c92f662b5c66145cde8245a1ac967130d7))
* sync command breakdown update and remove useless commands ([#622](https://github.com/Autodesk/pgbelt/issues/622)) ([558a28d](https://github.com/Autodesk/pgbelt/commit/558a28d0af3d577e0c6ecf5b06d431883b38fc2f))

## [0.7.11](https://github.com/Autodesk/pgbelt/compare/v0.7.10...v0.7.11) (2024-09-27)


### Bug Fixes

* accommodate Decimal NaN != Decimal NaN ([#573](https://github.com/Autodesk/pgbelt/issues/573)) ([3602266](https://github.com/Autodesk/pgbelt/commit/3602266ed1421dfe4efb5dabe8c9303e653f05d1))

## [0.7.10](https://github.com/Autodesk/pgbelt/compare/v0.7.9...v0.7.10) (2024-09-24)


### Bug Fixes

* add timestamps to logs DBAAS-92 ([#568](https://github.com/Autodesk/pgbelt/issues/568)) ([9c19378](https://github.com/Autodesk/pgbelt/commit/9c1937882343272e0e64887d2f57e9e5afa5eb6c))

## [0.7.9](https://github.com/Autodesk/pgbelt/compare/v0.7.8...v0.7.9) (2024-09-09)


### Bug Fixes

* support for capitals in column names ([#552](https://github.com/Autodesk/pgbelt/issues/552)) ([d0bea7b](https://github.com/Autodesk/pgbelt/commit/d0bea7b7c9e79664885cd2463d26e599b97472f9))

## [0.7.8](https://github.com/Autodesk/pgbelt/compare/v0.7.7...v0.7.8) (2024-09-09)


### Bug Fixes

* support for capital sequences and better sequence testing ([#550](https://github.com/Autodesk/pgbelt/issues/550)) ([48915e8](https://github.com/Autodesk/pgbelt/commit/48915e8efc55e9f48bb16fb25bdd1e7a2803cbd0))

## [0.7.7](https://github.com/Autodesk/pgbelt/compare/v0.7.6...v0.7.7) (2024-09-06)


### Bug Fixes

* support for capital letters in sequence names ([#548](https://github.com/Autodesk/pgbelt/issues/548)) ([885e234](https://github.com/Autodesk/pgbelt/commit/885e234e01becfac3eba2297d4618935760044b4))

## [0.7.6](https://github.com/Autodesk/pgbelt/compare/v0.7.5...v0.7.6) (2024-07-12)


### Bug Fixes

* support for dbs with no seqs, fixes [#515](https://github.com/Autodesk/pgbelt/issues/515) ([#516](https://github.com/Autodesk/pgbelt/issues/516)) ([5e9fe4c](https://github.com/Autodesk/pgbelt/commit/5e9fe4c8456c1a71f5440cba6e0d04c673d01f2d))

## [0.7.5](https://github.com/Autodesk/pgbelt/compare/v0.7.4...v0.7.5) (2024-07-10)


### Bug Fixes

* revoke-logins skips owner if owner is same as root [#509](https://github.com/Autodesk/pgbelt/issues/509) ([#510](https://github.com/Autodesk/pgbelt/issues/510)) ([ed7e758](https://github.com/Autodesk/pgbelt/commit/ed7e7588a710be696d857c86e46ec84e850dbff4))


### Documentation

* update quickstart and code comments to explain how to teardown properly ([#511](https://github.com/Autodesk/pgbelt/issues/511)) ([9f6a4a5](https://github.com/Autodesk/pgbelt/commit/9f6a4a593c4a20561b0efd5b0f112b7523288a53))

## [0.7.4](https://github.com/Autodesk/pgbelt/compare/v0.7.3...v0.7.4) (2024-04-08)


### Bug Fixes

* support for password special characters, fixes [#440](https://github.com/Autodesk/pgbelt/issues/440) ([#441](https://github.com/Autodesk/pgbelt/issues/441)) ([71b0f9d](https://github.com/Autodesk/pgbelt/commit/71b0f9d41add54967420c31ad86ac38b10ccdf2a))

## [0.7.3](https://github.com/Autodesk/pgbelt/compare/v0.7.2...v0.7.3) (2024-04-01)


### Bug Fixes

* actual table capitalization support - integration tests didn't actually have capitals inside the DBs ([99c78b7](https://github.com/Autodesk/pgbelt/commit/99c78b7dfb916ebf04d662c496ba62dc3dba839e))
* actual table capitalization support ([#436](https://github.com/Autodesk/pgbelt/issues/436)) ([99c78b7](https://github.com/Autodesk/pgbelt/commit/99c78b7dfb916ebf04d662c496ba62dc3dba839e))

## [0.7.2](https://github.com/Autodesk/pgbelt/compare/v0.7.1...v0.7.2) (2024-03-19)


### Bug Fixes

* support for table names with capitals ([#422](https://github.com/Autodesk/pgbelt/issues/422)) ([4512f2f](https://github.com/Autodesk/pgbelt/commit/4512f2f083cde6208ad344b61bd6e41e96745bf9))

## [0.7.1](https://github.com/Autodesk/pgbelt/compare/v0.7.0...v0.7.1) (2024-03-01)


### Bug Fixes

* various small fixes found from new integration testing ([#409](https://github.com/Autodesk/pgbelt/issues/409)) ([0309c56](https://github.com/Autodesk/pgbelt/commit/0309c5646c2d58e8d9c41299e4762a02c211f512))

## [0.7.0](https://github.com/Autodesk/pgbelt/compare/v0.6.2...v0.7.0) (2024-02-27)


### Features

* add percent progress to initializing status view ([#387](https://github.com/Autodesk/pgbelt/issues/387)) ([6139cfc](https://github.com/Autodesk/pgbelt/commit/6139cfc25d8ce4a272adf0587085455f136cdb66))
* handle exodus-style migrations ([#377](https://github.com/Autodesk/pgbelt/issues/377)) ([09c6887](https://github.com/Autodesk/pgbelt/commit/09c68878a4332698393956d5ee166134b84da8af))
* pgbelt now supports non-public schemas! ([#398](https://github.com/Autodesk/pgbelt/issues/398)) ([1f1fa87](https://github.com/Autodesk/pgbelt/commit/1f1fa87e6f884fd6ae16de7d032d4d79d24929ba))
* precheck now compares extensions of DBs ([#406](https://github.com/Autodesk/pgbelt/issues/406)) ([b74ce43](https://github.com/Autodesk/pgbelt/commit/b74ce438855ebf611672ccdfa7c0768fd4f6cd58))


### Bug Fixes

* sizes in status to should target config dataset instead of whole DB ([#395](https://github.com/Autodesk/pgbelt/issues/395)) ([80bf6ba](https://github.com/Autodesk/pgbelt/commit/80bf6bac8532ea57cd867597203392bf23d78f63))

## [0.6.2](https://github.com/Autodesk/pgbelt/compare/v0.6.1...v0.6.2) (2024-02-02)

### Bug Fixes

- stop using default replication set ([#371](https://github.com/Autodesk/pgbelt/issues/371)) ([cfa6276](https://github.com/Autodesk/pgbelt/commit/cfa62766c43c4f632582730339637be2f5021390))

## [0.6.1](https://github.com/Autodesk/pgbelt/compare/v0.6.0...v0.6.1) (2024-01-22)

### Bug Fixes

- pglogical teardown will now always revoke all from all tables from pglogical role ([#362](https://github.com/Autodesk/pgbelt/issues/362)) ([89e892c](https://github.com/Autodesk/pgbelt/commit/89e892c323ffda4bfbedd4d4a04ec612202cbe81))

## [0.6.0](https://github.com/Autodesk/pgbelt/compare/v0.5.4...v0.6.0) (2023-11-20)

### Features

- refactor Teardown Command due to DROP EXTENSION woes ([#326](https://github.com/Autodesk/pgbelt/issues/326)) ([5de6300](https://github.com/Autodesk/pgbelt/commit/5de6300d597f3e3f45dd03b83c286fcc07b1ced0))

## [0.5.4](https://github.com/Autodesk/pgbelt/compare/v0.5.3...v0.5.4) (2023-11-09)

### Bug Fixes

- bump pydantic from 1.10.13 to 2.4.2 ([#299](https://github.com/Autodesk/pgbelt/issues/299)) ([8597fcc](https://github.com/Autodesk/pgbelt/commit/8597fccca106606dd1513839bc23c170383f3f2b))

## [0.5.3](https://github.com/Autodesk/pgbelt/compare/v0.5.2...v0.5.3) (2023-11-09)

### Bug Fixes

- grant pglogical schema usage [#278](https://github.com/Autodesk/pgbelt/issues/278) ([#318](https://github.com/Autodesk/pgbelt/issues/318)) ([0de9290](https://github.com/Autodesk/pgbelt/commit/0de929074db26e5ca3d47ab9adfbf9bc3d055f0f))

## [0.5.2](https://github.com/Autodesk/pgbelt/compare/v0.5.1...v0.5.2) (2023-10-26)

### Bug Fixes

- remote.py error handling needs to shore up plugin error ([#311](https://github.com/Autodesk/pgbelt/issues/311)) ([b47b07f](https://github.com/Autodesk/pgbelt/commit/b47b07f6705b1c8548b67d17b69e5a29e2de5178))

## [0.5.1](https://github.com/Autodesk/pgbelt/compare/v0.5.0...v0.5.1) (2023-10-06)

### Bug Fixes

- index commands need to surface as CLI commands too ([#275](https://github.com/Autodesk/pgbelt/issues/275)) ([a8d6fae](https://github.com/Autodesk/pgbelt/commit/a8d6faef753d434a6883aac5797c2b0cb952e86a))

## [0.5.0](https://github.com/Autodesk/pgbelt/compare/v0.4.0...v0.5.0) (2023-10-06)

### Features

- stripping indexes and adding them later, and testing ([#273](https://github.com/Autodesk/pgbelt/issues/273)) ([c730e78](https://github.com/Autodesk/pgbelt/commit/c730e7852fdf40001633fd074b5aac4128a48af2))

### Bug Fixes

- precheck parameter value guidelines updated for [#267](https://github.com/Autodesk/pgbelt/issues/267) ([#268](https://github.com/Autodesk/pgbelt/issues/268)) ([e423929](https://github.com/Autodesk/pgbelt/commit/e42392993da1500ef564c3f6cc287f89531651e0))

## [0.4.0](https://github.com/Autodesk/pgbelt/compare/v0.3.0...v0.4.0) (2023-07-10)

### Features

- ability to not write logs to disk if needed ([#255](https://github.com/Autodesk/pgbelt/issues/255)) ([69469c0](https://github.com/Autodesk/pgbelt/commit/69469c0c16f1891992e7d15f4725369de3162cd7))

## [0.3.0](https://github.com/Autodesk/pgbelt/compare/v0.2.4...v0.3.0) (2023-06-29)

### Features

- check-connectivity command ([#253](https://github.com/Autodesk/pgbelt/issues/253)) ([6ebfff7](https://github.com/Autodesk/pgbelt/commit/6ebfff7a80573a1a33f1c54096fe300786e205c3))

## [0.2.4](https://github.com/Autodesk/pgbelt/compare/v0.2.3...v0.2.4) (2023-06-15)

### Bug Fixes

- allowing backwards compatibility down to python 3.9 ([#247](https://github.com/Autodesk/pgbelt/issues/247)) ([78b2fd8](https://github.com/Autodesk/pgbelt/commit/78b2fd8d05368f23ecd6c1ece223893bb424731c))

## [0.2.3](https://github.com/Autodesk/pgbelt/compare/v0.2.2...v0.2.3) (2023-05-04)

### Bug Fixes

- double quotes on table names ([#228](https://github.com/Autodesk/pgbelt/issues/228)) ([893773a](https://github.com/Autodesk/pgbelt/commit/893773aa113b719f32e4e5486e585c584499cd9e))

## [0.2.2](https://github.com/Autodesk/pgbelt/compare/v0.2.1...v0.2.2) (2023-05-02)

### Bug Fixes

- undo bump of virtualenv, back to 20.21.0 ([#223](https://github.com/Autodesk/pgbelt/issues/223)) ([01bfe6f](https://github.com/Autodesk/pgbelt/commit/01bfe6fc167df009d451ac83666f1075a4a0ae05))

## [0.2.1](https://github.com/Autodesk/pgbelt/compare/v0.2.0...v0.2.1) (2023-05-02)

### Bug Fixes

- typer doesn't support union types from python 3.11 ([#220](https://github.com/Autodesk/pgbelt/issues/220)) ([0315390](https://github.com/Autodesk/pgbelt/commit/03153909bcdf4e621b3e7c66aa5a99e55c9ceea4))

## [0.2.0](https://github.com/Autodesk/pgbelt/compare/v0.1.2...v0.2.0) (2023-04-28)

### Features

- update to python 3.11 ([#191](https://github.com/Autodesk/pgbelt/issues/191)) ([3c44681](https://github.com/Autodesk/pgbelt/commit/3c44681bfda3679bff10022eb9bfcb56fdd9e50a))

### Bug Fixes

- incorrect precheck command in quickstart.md ([#172](https://github.com/Autodesk/pgbelt/issues/172)) ([36ce96f](https://github.com/Autodesk/pgbelt/commit/36ce96feb6f5c98bcb3c7a0648a25e4f3b9ce655))
- no need to find and replace owners in schema ([#215](https://github.com/Autodesk/pgbelt/issues/215)) ([dfe1324](https://github.com/Autodesk/pgbelt/commit/dfe1324a56f57e75fa44dfe7196e104c34988695))

## [0.1.2](https://github.com/Autodesk/pgbelt/compare/v0.1.1...v0.1.2) (2022-10-03)

### Bug Fixes

- the analyze function should be run as root of a database for safest and least restriction [#87](https://github.com/Autodesk/pgbelt/issues/87) ([#90](https://github.com/Autodesk/pgbelt/issues/90)) ([81c5fde](https://github.com/Autodesk/pgbelt/commit/81c5fde8d1db4be797389a0ff361c00ee3329ab2))
- validator supposed to check 100 rows, not 1 ([#93](https://github.com/Autodesk/pgbelt/issues/93)) ([48122dc](https://github.com/Autodesk/pgbelt/commit/48122dca57b51b3f8114380840e510d70c75eaae))

## [0.1.1](https://github.com/Autodesk/pgbelt/compare/v0.1.0...v0.1.1) (2022-09-30)

### Documentation

- update README and quickstart now that pgbelt is on pypi ([#79](https://github.com/Autodesk/pgbelt/issues/79)) ([2a2d4c9](https://github.com/Autodesk/pgbelt/commit/2a2d4c91aaa8e961c2958df2f6150982b8b66c77))

## 0.1.0 (2022-09-02)

### Features

- convert to poetry ([07f9a10](https://github.com/Autodesk/pgbelt/commit/07f9a102aed7392e91dc7bffddb41e2bdf69eb52))
- fresh upload of pgbelt with GHA CI ([4df9896](https://github.com/Autodesk/pgbelt/commit/4df98960d662a14c99bb2c60ec80ba3c0317b22b))

### Bug Fixes

- ci move to poetry too ([979c988](https://github.com/Autodesk/pgbelt/commit/979c988666dea0705cecc19fdd7cf941f17f2d6b))
- dev setup in Makefile also installs precommit ([c27cb09](https://github.com/Autodesk/pgbelt/commit/c27cb09615b7a448f31ca07fb6cdab6fe8a10f0f))
- flake8 baited, placing ignore, it broke pydantic ([#17](https://github.com/Autodesk/pgbelt/issues/17)) ([560207d](https://github.com/Autodesk/pgbelt/commit/560207df79276ceece0d4df00221e748f2718648))
- need code of conduct for open-sourcing ([#58](https://github.com/Autodesk/pgbelt/issues/58)) ([4aede91](https://github.com/Autodesk/pgbelt/commit/4aede91a9c4344d3fcaa1972bc6b00cdf70f5ce6))
- pre-commit fixed and run ([d3e188b](https://github.com/Autodesk/pgbelt/commit/d3e188bc2b9641614e8e63a5011adecf4db511e2))
- pre-commit fixes and flake8 config ([#15](https://github.com/Autodesk/pgbelt/issues/15)) ([958afd0](https://github.com/Autodesk/pgbelt/commit/958afd04b5f542704b15809274d039d0accc3cbe))
- remove rando .git file ([#5](https://github.com/Autodesk/pgbelt/issues/5)) ([16d38fd](https://github.com/Autodesk/pgbelt/commit/16d38fd2a7c607552191444d69d0f02212aecfc9))
- remove random .git files ([35213a5](https://github.com/Autodesk/pgbelt/commit/35213a518ce4f3450f462aceb6d139b6379f4cc1))
- update black and fix ci poetry ([5a7a88b](https://github.com/Autodesk/pgbelt/commit/5a7a88beb4cac34a7c1e65f686f8cd0fd1f6ac50))
