# Changelog

[PyPI History][1]

[1]: https://pypi.org/project/google-cloud-bigtable/#history

## [2.11.3](https://github.com/googleapis/python-bigtable/compare/v2.11.2...v2.11.3) (2022-08-17)


### Performance Improvements

* optimize row merging ([#628](https://github.com/googleapis/python-bigtable/issues/628)) ([c71ec70](https://github.com/googleapis/python-bigtable/commit/c71ec70e55f6e236e46127870a9ed4717eee5da5))

## [2.11.2](https://github.com/googleapis/python-bigtable/compare/v2.11.1...v2.11.2) (2022-08-11)


### Bug Fixes

* **deps:** allow protobuf < 5.0.0 ([#631](https://github.com/googleapis/python-bigtable/issues/631)) ([fd54fc6](https://github.com/googleapis/python-bigtable/commit/fd54fc63340a3e01fae1ccc4c648dd90900f8a94))
* **deps:** require proto-plus >= 1.22.0 ([fd54fc6](https://github.com/googleapis/python-bigtable/commit/fd54fc63340a3e01fae1ccc4c648dd90900f8a94))

## [2.11.1](https://github.com/googleapis/python-bigtable/compare/v2.11.0...v2.11.1) (2022-08-08)


### Bug Fixes

* Retry the RST Stream error in mutate rows and read rows([#624](https://github.com/googleapis/python-bigtable/issues/624)) ([d24574a](https://github.com/googleapis/python-bigtable/commit/d24574a722de61bdeffa6588bcb08f56e62ba3bd))

## [2.11.0](https://github.com/googleapis/python-bigtable/compare/v2.10.1...v2.11.0) (2022-08-04)


### Features

* add audience parameter ([a7a7699](https://github.com/googleapis/python-bigtable/commit/a7a76998fad3c12215527e4ebb517a1526cc152e))
* add satisfies_pzs output only field ([#614](https://github.com/googleapis/python-bigtable/issues/614)) ([7dc1469](https://github.com/googleapis/python-bigtable/commit/7dc1469fef2dc38f1509b35a37e9c97381ab7601))
* Add storage_utilization_gib_per_node to Autoscaling target ([a7a7699](https://github.com/googleapis/python-bigtable/commit/a7a76998fad3c12215527e4ebb517a1526cc152e))
* Cloud Bigtable Undelete Table service and message proto files ([a7a7699](https://github.com/googleapis/python-bigtable/commit/a7a76998fad3c12215527e4ebb517a1526cc152e))


### Bug Fixes

* **deps:** require google-api-core>=1.32.0,>=2.8.0 ([a7a7699](https://github.com/googleapis/python-bigtable/commit/a7a76998fad3c12215527e4ebb517a1526cc152e))
* require python 3.7+ ([#610](https://github.com/googleapis/python-bigtable/issues/610)) ([10d00f5](https://github.com/googleapis/python-bigtable/commit/10d00f5af5d5878c26529f5e48a5fb8d8385696d))


### Performance Improvements

* improve row merging ([#619](https://github.com/googleapis/python-bigtable/issues/619)) ([b4853e5](https://github.com/googleapis/python-bigtable/commit/b4853e59d0efd8a7b37f3fcb06b14dbd9f5d20a4))

## [2.10.1](https://github.com/googleapis/python-bigtable/compare/v2.10.0...v2.10.1) (2022-06-03)


### Bug Fixes

* **deps:** require protobuf <4.0.0dev ([#595](https://github.com/googleapis/python-bigtable/issues/595)) ([a4deaf7](https://github.com/googleapis/python-bigtable/commit/a4deaf7b1b5c4b7ce8f6dc5bb96d32ea8ff55c2d))


### Documentation

* fix changelog header to consistent size ([#596](https://github.com/googleapis/python-bigtable/issues/596)) ([51961c3](https://github.com/googleapis/python-bigtable/commit/51961c32686fe5851e957581b85adbe92a073e03))

## [2.10.0](https://github.com/googleapis/python-bigtable/compare/v2.9.0...v2.10.0) (2022-05-30)


### Features

* refreshes Bigtable Admin API(s) protos ([#589](https://github.com/googleapis/python-bigtable/issues/589)) ([b508e33](https://github.com/googleapis/python-bigtable/commit/b508e3321937850d65242283e82f5413feb6081a))


### Documentation

* Add EncryptionInfo documentation ([#588](https://github.com/googleapis/python-bigtable/issues/588)) ([bedbf1b](https://github.com/googleapis/python-bigtable/commit/bedbf1b1bb304ff45f31ad20004ff96041ce716c))

## [2.9.0](https://github.com/googleapis/python-bigtable/compare/v2.8.1...v2.9.0) (2022-04-14)


### Features

* App Profile multi cluster routing support with specified cluster ids ([#549](https://github.com/googleapis/python-bigtable/issues/549)) ([a0ed5b5](https://github.com/googleapis/python-bigtable/commit/a0ed5b5dfda1f3980b1a8eb349b2b5d8ab428a4b))
* AuditConfig for IAM v1 ([4e50278](https://github.com/googleapis/python-bigtable/commit/4e50278c73f608a7c493692d8d17e7dd2aa7ba44))


### Bug Fixes

* **deps:** require grpc-google-iam-v1 >=0.12.4 ([4e50278](https://github.com/googleapis/python-bigtable/commit/4e50278c73f608a7c493692d8d17e7dd2aa7ba44))


### Documentation

* fix type in docstring for map fields ([4e50278](https://github.com/googleapis/python-bigtable/commit/4e50278c73f608a7c493692d8d17e7dd2aa7ba44))

## [2.8.1](https://github.com/googleapis/python-bigtable/compare/v2.8.0...v2.8.1) (2022-04-07)


### Bug Fixes

* Prevent sending full table scan when retrying ([#554](https://github.com/googleapis/python-bigtable/issues/554)) ([56f5357](https://github.com/googleapis/python-bigtable/commit/56f5357c09ac867491b934f6029776dcd74c6eac))

## [2.8.0](https://github.com/googleapis/python-bigtable/compare/v2.7.1...v2.8.0) (2022-04-04)


### Features

* Add ListHotTablets API method and protobufs ([#542](https://github.com/googleapis/python-bigtable/issues/542)) ([483f139](https://github.com/googleapis/python-bigtable/commit/483f139f5065d55378bd850c33e89db460119fc1))


### Documentation

* explain mutate vs mutate_rows ([#543](https://github.com/googleapis/python-bigtable/issues/543)) ([84cfb0a](https://github.com/googleapis/python-bigtable/commit/84cfb0abdfabd8aa2f292fc0bb7e6deab50f87f1))
* Remove the limitation that all clusters in a CMEK instance must use the same key ([f008eea](https://github.com/googleapis/python-bigtable/commit/f008eea69a6c7c1a027cefc7f16d46042b524db1))
* Update `cpu_utilization_percent` limit ([#547](https://github.com/googleapis/python-bigtable/issues/547)) ([f008eea](https://github.com/googleapis/python-bigtable/commit/f008eea69a6c7c1a027cefc7f16d46042b524db1))

## [2.7.1](https://github.com/googleapis/python-bigtable/compare/v2.7.0...v2.7.1) (2022-03-17)


### Bug Fixes

* Ensure message fields are copied when building retry request ([#533](https://github.com/googleapis/python-bigtable/issues/533)) ([ff7f190](https://github.com/googleapis/python-bigtable/commit/ff7f1901b6420e66e1388e757eeec20d30484ad9))

## [2.7.0](https://github.com/googleapis/python-bigtable/compare/v2.6.0...v2.7.0) (2022-03-06)


### Features

* Add support for autoscaling ([#509](https://github.com/googleapis/python-bigtable/issues/509)) ([8f4e197](https://github.com/googleapis/python-bigtable/commit/8f4e197148644ded934190814ff44fa132a2dda6))


### Bug Fixes

* **deps:** require google-api-core>=1.31.5, >=2.3.2 ([#526](https://github.com/googleapis/python-bigtable/issues/526)) ([a8a92ee](https://github.com/googleapis/python-bigtable/commit/a8a92ee1b6bd284055fee3e1029a9a6aacbc5f1c))
* **deps:** require proto-plus>=1.15.0 ([a8a92ee](https://github.com/googleapis/python-bigtable/commit/a8a92ee1b6bd284055fee3e1029a9a6aacbc5f1c))

## [2.6.0](https://github.com/googleapis/python-bigtable/compare/v2.5.2...v2.6.0) (2022-02-26)


### Features

* add WarmAndPing request for channel priming ([#504](https://github.com/googleapis/python-bigtable/issues/504)) ([df5fc1f](https://github.com/googleapis/python-bigtable/commit/df5fc1f7d6ded88d9bce67f7cc6989981745931f))

## [2.5.2](https://github.com/googleapis/python-bigtable/compare/v2.5.1...v2.5.2) (2022-02-24)


### Bug Fixes

* Pass app_profile_id when building updated request ([#512](https://github.com/googleapis/python-bigtable/issues/512)) ([2f8ba7a](https://github.com/googleapis/python-bigtable/commit/2f8ba7a4801b17b5afb6180a7ace1327a2d05a52))

## [2.5.1](https://github.com/googleapis/python-bigtable/compare/v2.5.0...v2.5.1) (2022-02-17)


### Bug Fixes

* **deps:** move libcst to extras ([#508](https://github.com/googleapis/python-bigtable/issues/508)) ([4b4d7e2](https://github.com/googleapis/python-bigtable/commit/4b4d7e2796788b2cd3764f54ff532a9c9d092aec))

## [2.5.0](https://github.com/googleapis/python-bigtable/compare/v2.4.0...v2.5.0) (2022-02-07)


### Features

* add 'Instance.create_time' field ([#449](https://github.com/googleapis/python-bigtable/issues/449)) ([b9ecfa9](https://github.com/googleapis/python-bigtable/commit/b9ecfa97281ae21dcf233e60c70cacc701f12c32))
* add api key support ([#497](https://github.com/googleapis/python-bigtable/issues/497)) ([ee3a6c4](https://github.com/googleapis/python-bigtable/commit/ee3a6c4c5f810fab08671db3407195864ecc1972))
* add Autoscaling API ([#475](https://github.com/googleapis/python-bigtable/issues/475)) ([97b3cdd](https://github.com/googleapis/python-bigtable/commit/97b3cddb908098e255e7a1209cdb985087b95a26))
* add context manager support in client ([#440](https://github.com/googleapis/python-bigtable/issues/440)) ([a3d2cf1](https://github.com/googleapis/python-bigtable/commit/a3d2cf18b49cddc91e5e6448c46d6b936d86954d))
* add support for Python 3.10 ([#437](https://github.com/googleapis/python-bigtable/issues/437)) ([3cf0814](https://github.com/googleapis/python-bigtable/commit/3cf08149411f3f4df41e9b5a9894dbfb101bd86f))


### Bug Fixes

* **deps:** drop packaging dependency ([a535f99](https://github.com/googleapis/python-bigtable/commit/a535f99e9f0bb16488a5d372a0a6efc3c4b69186))
* **deps:** require google-api-core >= 1.28.0 ([a535f99](https://github.com/googleapis/python-bigtable/commit/a535f99e9f0bb16488a5d372a0a6efc3c4b69186))
* improper types in pagers generation ([f9c7699](https://github.com/googleapis/python-bigtable/commit/f9c7699eb6d4071314abbb0477ba47370059e041))
* improve type hints, mypy checks ([#448](https://github.com/googleapis/python-bigtable/issues/448)) ([a99bf88](https://github.com/googleapis/python-bigtable/commit/a99bf88417d6aec03923447c70c2752f6bb5c459))
* resolve DuplicateCredentialArgs error when using credentials_file ([d6bff70](https://github.com/googleapis/python-bigtable/commit/d6bff70654b41e31d2ac83d307bdc6bbd111201e))


### Documentation

* clarify comments in ReadRowsRequest and RowFilter ([#494](https://github.com/googleapis/python-bigtable/issues/494)) ([1efd9b5](https://github.com/googleapis/python-bigtable/commit/1efd9b598802f766a3c4c8c78ec7b0ca208d3325))
* list oneofs in docstring ([a535f99](https://github.com/googleapis/python-bigtable/commit/a535f99e9f0bb16488a5d372a0a6efc3c4b69186))

## [2.4.0](https://www.github.com/googleapis/python-bigtable/compare/v2.3.3...v2.4.0) (2021-09-24)


### Features

* Publish new fields to support cluster group routing for Cloud Bigtable ([#407](https://www.github.com/googleapis/python-bigtable/issues/407)) ([66af554](https://www.github.com/googleapis/python-bigtable/commit/66af554a103eea0139cb313691d69f4c88a9e87f))


### Bug Fixes

* add 'dict' annotation type to 'request' ([160bfd3](https://www.github.com/googleapis/python-bigtable/commit/160bfd317a83561821acc0212d3514701a031ac6))

## [2.3.3](https://www.github.com/googleapis/python-bigtable/compare/v2.3.2...v2.3.3) (2021-07-24)


### Bug Fixes

* enable self signed jwt for grpc ([#397](https://www.github.com/googleapis/python-bigtable/issues/397)) ([9d43a38](https://www.github.com/googleapis/python-bigtable/commit/9d43a388470746608d324ca8d72f41bb3a4492b7))

## [2.3.2](https://www.github.com/googleapis/python-bigtable/compare/v2.3.1...v2.3.2) (2021-07-20)


### Bug Fixes

* **deps:** pin 'google-{api,cloud}-core', 'google-auth' to allow 2.x versions ([#379](https://www.github.com/googleapis/python-bigtable/issues/379)) ([95b2e13](https://www.github.com/googleapis/python-bigtable/commit/95b2e13b776dca4a6998313c41aa960ffe2e47e9))
* directly append to pb for beter read row performance ([#382](https://www.github.com/googleapis/python-bigtable/issues/382)) ([7040e11](https://www.github.com/googleapis/python-bigtable/commit/7040e113b93bb2e0625c054486305235d8f14c2a))

## [2.3.1](https://www.github.com/googleapis/python-bigtable/compare/v2.3.0...v2.3.1) (2021-07-13)


### Bug Fixes

* use public 'table_admin_client' property in backups methods ([#359](https://www.github.com/googleapis/python-bigtable/issues/359)) ([bc57c79](https://www.github.com/googleapis/python-bigtable/commit/bc57c79640b270ff89fd10ec243dd04559168c5c))

## [2.3.0](https://www.github.com/googleapis/python-bigtable/compare/v2.2.0...v2.3.0) (2021-07-01)


### Features

* add always_use_jwt_access ([#333](https://www.github.com/googleapis/python-bigtable/issues/333)) ([f1fce5b](https://www.github.com/googleapis/python-bigtable/commit/f1fce5b0694d965202fc2a4fcf8bc6e09e78deae))


### Bug Fixes

* **deps:** add packaging requirement ([#326](https://www.github.com/googleapis/python-bigtable/issues/326)) ([d31c27b](https://www.github.com/googleapis/python-bigtable/commit/d31c27b01d1f7c351effc2856a8d4777a1a10690))
* **deps:** require google-api-core >= 1.26.0 ([#344](https://www.github.com/googleapis/python-bigtable/issues/344)) ([ce4ceb6](https://www.github.com/googleapis/python-bigtable/commit/ce4ceb6d8fe74eff16cf9ca151e0b98502256a2f))
* disable always_use_jwt_access ([#348](https://www.github.com/googleapis/python-bigtable/issues/348)) ([4623248](https://www.github.com/googleapis/python-bigtable/commit/4623248376deccf4651d4badf8966311ebe3c16a))


### Documentation

* add paramter mutation_timeout to instance.table docs ([#305](https://www.github.com/googleapis/python-bigtable/issues/305)) ([5bbd06e](https://www.github.com/googleapis/python-bigtable/commit/5bbd06e5413e8b7597ba128174b10fe45fd38380))
* fix broken links in multiprocessing.rst ([#317](https://www.github.com/googleapis/python-bigtable/issues/317)) ([e329352](https://www.github.com/googleapis/python-bigtable/commit/e329352d7e6d81de1d1d770c73406a60d29d01bb))
* omit mention of Python 2.7 in 'CONTRIBUTING.rst' ([#1127](https://www.github.com/googleapis/python-bigtable/issues/1127)) ([#329](https://www.github.com/googleapis/python-bigtable/issues/329)) ([6bf0c64](https://www.github.com/googleapis/python-bigtable/commit/6bf0c647bcebed641b4cbdc5eb70528c88b26a01)), closes [#1126](https://www.github.com/googleapis/python-bigtable/issues/1126)

## [2.2.0](https://www.github.com/googleapis/python-bigtable/compare/v2.1.0...v2.2.0) (2021-04-30)


### Features

* backup restore to different instance ([#300](https://www.github.com/googleapis/python-bigtable/issues/300)) ([049a25f](https://www.github.com/googleapis/python-bigtable/commit/049a25f903bb6b062e41430b6e7ce6d7b164f22c))

## [2.1.0](https://www.github.com/googleapis/python-bigtable/compare/v2.0.0...v2.1.0) (2021-04-21)


### Features

* customer managed keys (CMEK) ([#249](https://www.github.com/googleapis/python-bigtable/issues/249)) ([93df829](https://www.github.com/googleapis/python-bigtable/commit/93df82998cc0218cbc4a1bc2ab41a48b7478758d))

## [2.0.0](https://www.github.com/googleapis/python-bigtable/compare/v1.7.0...v2.0.0) (2021-04-06)


### ⚠ BREAKING CHANGES

* microgenerator changes (#203)

### Features

* microgenerator changes ([#203](https://www.github.com/googleapis/python-bigtable/issues/203)) ([b31bd87](https://www.github.com/googleapis/python-bigtable/commit/b31bd87c3fa8cad32768611a52d5effcc7d9b3e2))
* publish new fields for CMEK ([#222](https://www.github.com/googleapis/python-bigtable/issues/222)) ([0fe5b63](https://www.github.com/googleapis/python-bigtable/commit/0fe5b638e45e711d25f55664689a9baf4d12dc57))


### Bug Fixes

* address issue in establishing an emulator connection ([#246](https://www.github.com/googleapis/python-bigtable/issues/246)) ([1a31826](https://www.github.com/googleapis/python-bigtable/commit/1a31826e2e378468e057160c07d850ebca1c5879))
* fix unit test that could be broken by user's environment ([#239](https://www.github.com/googleapis/python-bigtable/issues/239)) ([cbd712e](https://www.github.com/googleapis/python-bigtable/commit/cbd712e6d3aded0c025525f97da1d667fbe2f061))
* guard assignments of certain values against None ([#220](https://www.github.com/googleapis/python-bigtable/issues/220)) ([341f448](https://www.github.com/googleapis/python-bigtable/commit/341f448ce378375ab79bfc82f864fb6c88ed71a0))
* **retry:** restore grpc_service_config for CreateBackup and {Restore,Snapshot}Table ([#240](https://www.github.com/googleapis/python-bigtable/issues/240)) ([79f1734](https://www.github.com/googleapis/python-bigtable/commit/79f1734c897e5e1b2fd02d043185c44b7ee34dc9))


### Documentation

* add backup docs ([#251](https://www.github.com/googleapis/python-bigtable/issues/251)) ([7d5c7aa](https://www.github.com/googleapis/python-bigtable/commit/7d5c7aa92cb476b07ac9efb5d231888c4c417783))


### Dependencies

* update gapic-generator-python to 0.40.11 ([#230](https://www.github.com/googleapis/python-bigtable/issues/230)) ([47d5dc1](https://www.github.com/googleapis/python-bigtable/commit/47d5dc1853f0be609e666e8a8fad0146f2905482))
* upgrade gapic-generator-python to 0.43.1 ([#276](https://www.github.com/googleapis/python-bigtable/issues/276)) ([0e9fe54](https://www.github.com/googleapis/python-bigtable/commit/0e9fe5410e1b5d16ae0735ba1f606f7d1befafb9))

## [2.0.0-dev1](https://www.github.com/googleapis/python-bigtable/compare/v1.7.0...v2.0.0-dev1) (2021-02-24)


### ⚠ BREAKING CHANGES

* microgenerator changes (#203)

### Features

* microgenerator changes ([#203](https://www.github.com/googleapis/python-bigtable/issues/203)) ([b31bd87](https://www.github.com/googleapis/python-bigtable/commit/b31bd87c3fa8cad32768611a52d5effcc7d9b3e2))


### Bug Fixes

* guard assignments of certain values against None ([#220](https://www.github.com/googleapis/python-bigtable/issues/220)) ([341f448](https://www.github.com/googleapis/python-bigtable/commit/341f448ce378375ab79bfc82f864fb6c88ed71a0))

## [1.7.0](https://www.github.com/googleapis/python-bigtable/compare/v1.6.1...v1.7.0) (2021-02-09)


### Features

* add keep alive timeout ([#182](https://www.github.com/googleapis/python-bigtable/issues/182)) ([e9637cb](https://www.github.com/googleapis/python-bigtable/commit/e9637cbd4461dcca509dca43ef116d6ff41b80c7))
* support filtering on incrementable values ([#178](https://www.github.com/googleapis/python-bigtable/issues/178)) ([e221352](https://www.github.com/googleapis/python-bigtable/commit/e2213520951d3da97019a1d784e5bf31d94e3353))


### Bug Fixes

* Renaming region tags to not conflict with documentation snippets ([#190](https://www.github.com/googleapis/python-bigtable/issues/190)) ([dd0cdc5](https://www.github.com/googleapis/python-bigtable/commit/dd0cdc5bcfd92e18ab9a7255684a9f5b21198867))


### Documentation

* update python contributing guide ([#206](https://www.github.com/googleapis/python-bigtable/issues/206)) ([e301ac3](https://www.github.com/googleapis/python-bigtable/commit/e301ac3b61364d779fdb50a57ae8e2cb9952df9e))

## [1.6.1](https://www.github.com/googleapis/python-bigtable/compare/v1.6.0...v1.6.1) (2020-12-01)


### Documentation

* update intersphinx mappings ([#172](https://www.github.com/googleapis/python-bigtable/issues/172)) ([7b09368](https://www.github.com/googleapis/python-bigtable/commit/7b09368d5121782c7f271b3575c838e8a2284c05))

## [1.6.0](https://www.github.com/googleapis/python-bigtable/compare/v1.5.1...v1.6.0) (2020-11-16)


### Features

* add 'timeout' arg to 'Table.mutate_rows' ([#157](https://www.github.com/googleapis/python-bigtable/issues/157)) ([6d597a1](https://www.github.com/googleapis/python-bigtable/commit/6d597a1e5be05c993c9f86beca4c1486342caf94)), closes [/github.com/googleapis/python-bigtable/issues/7#issuecomment-715538708](https://www.github.com/googleapis//github.com/googleapis/python-bigtable/issues/7/issues/issuecomment-715538708) [#7](https://www.github.com/googleapis/python-bigtable/issues/7)
* Backup Level IAM ([#160](https://www.github.com/googleapis/python-bigtable/issues/160)) ([44932cb](https://www.github.com/googleapis/python-bigtable/commit/44932cb8710e12279dbd4e9271577f8bee238980))

## [1.5.1](https://www.github.com/googleapis/python-bigtable/compare/v1.5.0...v1.5.1) (2020-10-06)


### Bug Fixes

* harden version data gathering against DistributionNotFound ([#150](https://www.github.com/googleapis/python-bigtable/issues/150)) ([c815421](https://www.github.com/googleapis/python-bigtable/commit/c815421422f1c845983e174651a5292767cfe2e7))

## [1.5.0](https://www.github.com/googleapis/python-bigtable/compare/v1.4.0...v1.5.0) (2020-09-22)


### Features

* add 'Rowset.add_row_range_with_prefix' ([#30](https://www.github.com/googleapis/python-bigtable/issues/30)) ([4796ac8](https://www.github.com/googleapis/python-bigtable/commit/4796ac85c877d75ed596cde7628dae31918ef726))
* add response status to DirectRow.commit() ([#128](https://www.github.com/googleapis/python-bigtable/issues/128)) ([2478bb8](https://www.github.com/googleapis/python-bigtable/commit/2478bb864adbc71ef606e2b10b3bdfe3a7d44717)), closes [#127](https://www.github.com/googleapis/python-bigtable/issues/127)
* pass 'client_options' to base class ctor ([#104](https://www.github.com/googleapis/python-bigtable/issues/104)) ([e55ca07](https://www.github.com/googleapis/python-bigtable/commit/e55ca07561f9c946276f3bde599e69947769f560)), closes [#69](https://www.github.com/googleapis/python-bigtable/issues/69)


### Bug Fixes

* pass timeout to 'PartialRowsData.response_iterator' ([#16](https://www.github.com/googleapis/python-bigtable/issues/16)) ([8f76434](https://www.github.com/googleapis/python-bigtable/commit/8f764343e01d50ad880363f5a4e5630122cbdb25))
* retry if failure occurs on initial call in MutateRows ([#123](https://www.github.com/googleapis/python-bigtable/issues/123)) ([0c9cde8](https://www.github.com/googleapis/python-bigtable/commit/0c9cde8ade0e4f50d06bbbd1b4169ae5c545b2c0))
* **python_samples:** README link fix, enforce samples=True ([#114](https://www.github.com/googleapis/python-bigtable/issues/114)) ([dfe658a](https://www.github.com/googleapis/python-bigtable/commit/dfe658a2b1270eda7a8a084aca28d65b3297a04f))


### Documentation

* add sample for writing data with Beam ([#80](https://www.github.com/googleapis/python-bigtable/issues/80)) ([6900290](https://www.github.com/googleapis/python-bigtable/commit/6900290e00daf04ca545284b3f0a591a2de11136))
* clarify 'Table.read_rows' snippet ([#50](https://www.github.com/googleapis/python-bigtable/issues/50)) ([5ca8bbd](https://www.github.com/googleapis/python-bigtable/commit/5ca8bbd0fb9c4a7cef7b4cbb67d1ba9f2382f2d8))
* document 'row_set' module explicitly ([#29](https://www.github.com/googleapis/python-bigtable/issues/29)) ([0e0291e](https://www.github.com/googleapis/python-bigtable/commit/0e0291e56cbaeec00ede5275e17af2968a12251c))
* Pysamples new readme gen ([#112](https://www.github.com/googleapis/python-bigtable/issues/112)) ([3ecca7a](https://www.github.com/googleapis/python-bigtable/commit/3ecca7a7b52b0f4fc38db5c5016622b994c1a8aa))
* remove indent from snippet code blocks ([#49](https://www.github.com/googleapis/python-bigtable/issues/49)) ([1fbadf9](https://www.github.com/googleapis/python-bigtable/commit/1fbadf906204c622b9cff3fa073d8fc43d3597f7))
* switch links to client documentation ([#93](https://www.github.com/googleapis/python-bigtable/issues/93)) ([2c973e6](https://www.github.com/googleapis/python-bigtable/commit/2c973e6cce969e7003be0b3d7a164bdc61b91ef1))
* update docs build (via synth) ([#99](https://www.github.com/googleapis/python-bigtable/issues/99)) ([c301b53](https://www.github.com/googleapis/python-bigtable/commit/c301b53db4f7d48fd76548a5cd3a01cc46ff1522)), closes [#700](https://www.github.com/googleapis/python-bigtable/issues/700)
* update links to reflect new Github org ([#48](https://www.github.com/googleapis/python-bigtable/issues/48)) ([9bb11ed](https://www.github.com/googleapis/python-bigtable/commit/9bb11edc885958286b5b31fa18cfd0db95338cb4))
* use correct storage type constant in docstrings ([#110](https://www.github.com/googleapis/python-bigtable/issues/110)) ([bc6db77](https://www.github.com/googleapis/python-bigtable/commit/bc6db77809a89fd6f3b2095cfe9b84d2da1bf304))
* **samples:** filter cpu query to get metrics for the correct resources [([#4238](https://www.github.com/googleapis/python-bigtable/issues/4238))](https://github.com/GoogleCloudPlatform/python-docs-samples/issues/4238) ([#81](https://www.github.com/googleapis/python-bigtable/issues/81)) ([2c8c386](https://www.github.com/googleapis/python-bigtable/commit/2c8c3864c43a7ac9c85a0cd7c9cd4eec7434b42d))

## [1.4.0](https://www.github.com/googleapis/python-bigtable/compare/v1.3.0...v1.4.0) (2020-07-21)


### Features

* **bigtable:** Managed Backups wrappers ([#57](https://www.github.com/googleapis/python-bigtable/issues/57)) ([a351734](https://www.github.com/googleapis/python-bigtable/commit/a351734ae16b4a689b89e6a42f63ea3ea5ad84ca))

## [1.3.0](https://www.github.com/googleapis/python-bigtable/compare/v1.2.1...v1.3.0) (2020-07-16)


### Features

* **api_core:** support version 3 policy bindings ([#9869](https://www.github.com/googleapis/python-bigtable/issues/9869)) ([a9dee32](https://www.github.com/googleapis/python-bigtable/commit/a9dee327ab39e22a014b3c4126f1c9d1beebe2d1))
* **bigtable:** add py2 deprecation warnings; standardize use of 'required' in docstrings (via synth) ([#10064](https://www.github.com/googleapis/python-bigtable/issues/10064)) ([5460de0](https://www.github.com/googleapis/python-bigtable/commit/5460de0f7e0d936a23289f679c2b1a3040a21247))
* Create CODEOWNERS ([#27](https://www.github.com/googleapis/python-bigtable/issues/27)) ([2b63746](https://www.github.com/googleapis/python-bigtable/commit/2b6374600d911b3dfd567eafd964260eb00a2bc0))
* **bigtable:** skip system tests failing with emulator ([#18](https://www.github.com/googleapis/python-bigtable/issues/18)) ([399d3d3](https://www.github.com/googleapis/python-bigtable/commit/399d3d3f960786f616ab6085f142a9703b0391e0))
* **bigtable:** support requested_policy_version for Instance IAM ([#10001](https://www.github.com/googleapis/python-bigtable/issues/10001)) ([7e5d963](https://www.github.com/googleapis/python-bigtable/commit/7e5d963857fd8f7547778d5247b53c24de7a43f6)), closes [#3](https://www.github.com/googleapis/python-bigtable/issues/3)
* update gapic-generator and go microgen, backups generated api ([#55](https://www.github.com/googleapis/python-bigtable/issues/55)) ([c38888d](https://www.github.com/googleapis/python-bigtable/commit/c38888de3d0b1c49c438a7d350f42bc1805809f2))


### Bug Fixes

* localdeps ([5d799b2](https://www.github.com/googleapis/python-bigtable/commit/5d799b2d99e79ee9d20ae6cf2663d670493a8db3))
* test_utils ([43481a9](https://www.github.com/googleapis/python-bigtable/commit/43481a91275e93fadd22eaa7cba3891a00cb97f8))
* **python:** change autodoc_default_flags to autodoc_default_options ([#58](https://www.github.com/googleapis/python-bigtable/issues/58)) ([5c1d618](https://www.github.com/googleapis/python-bigtable/commit/5c1d61827618d254c453b3871c0022a8d35bfbb2))


### Documentation

* add note about multiprocessing usage ([#26](https://www.github.com/googleapis/python-bigtable/issues/26)) ([1449589](https://www.github.com/googleapis/python-bigtable/commit/1449589e8b5b9037dae4e9b071ff7e7662992e18))
* **bigtable:** clean up ([#32](https://www.github.com/googleapis/python-bigtable/issues/32)) ([9f4068c](https://www.github.com/googleapis/python-bigtable/commit/9f4068cf8eb4351c02a4862380547ecf2564d838))
* add samples from bigtable ([#38](https://www.github.com/googleapis/python-bigtable/issues/38)) ([1121f0d](https://www.github.com/googleapis/python-bigtable/commit/1121f0d647dbfc6c70a459b0979465803fdfad7b)), closes [#371](https://www.github.com/googleapis/python-bigtable/issues/371) [#383](https://www.github.com/googleapis/python-bigtable/issues/383) [#383](https://www.github.com/googleapis/python-bigtable/issues/383) [#456](https://www.github.com/googleapis/python-bigtable/issues/456) [#456](https://www.github.com/googleapis/python-bigtable/issues/456) [#540](https://www.github.com/googleapis/python-bigtable/issues/540) [#540](https://www.github.com/googleapis/python-bigtable/issues/540) [#542](https://www.github.com/googleapis/python-bigtable/issues/542) [#542](https://www.github.com/googleapis/python-bigtable/issues/542) [#544](https://www.github.com/googleapis/python-bigtable/issues/544) [#544](https://www.github.com/googleapis/python-bigtable/issues/544) [#576](https://www.github.com/googleapis/python-bigtable/issues/576) [#599](https://www.github.com/googleapis/python-bigtable/issues/599) [#599](https://www.github.com/googleapis/python-bigtable/issues/599) [#656](https://www.github.com/googleapis/python-bigtable/issues/656) [#715](https://www.github.com/googleapis/python-bigtable/issues/715) [#715](https://www.github.com/googleapis/python-bigtable/issues/715) [#781](https://www.github.com/googleapis/python-bigtable/issues/781) [#781](https://www.github.com/googleapis/python-bigtable/issues/781) [#887](https://www.github.com/googleapis/python-bigtable/issues/887) [#887](https://www.github.com/googleapis/python-bigtable/issues/887) [#914](https://www.github.com/googleapis/python-bigtable/issues/914) [#914](https://www.github.com/googleapis/python-bigtable/issues/914) [#922](https://www.github.com/googleapis/python-bigtable/issues/922) [#922](https://www.github.com/googleapis/python-bigtable/issues/922) [#962](https://www.github.com/googleapis/python-bigtable/issues/962) [#962](https://www.github.com/googleapis/python-bigtable/issues/962) [#1004](https://www.github.com/googleapis/python-bigtable/issues/1004) [#1004](https://www.github.com/googleapis/python-bigtable/issues/1004) [#1003](https://www.github.com/googleapis/python-bigtable/issues/1003) [#1005](https://www.github.com/googleapis/python-bigtable/issues/1005) [#1005](https://www.github.com/googleapis/python-bigtable/issues/1005) [#1028](https://www.github.com/googleapis/python-bigtable/issues/1028) [#1055](https://www.github.com/googleapis/python-bigtable/issues/1055) [#1055](https://www.github.com/googleapis/python-bigtable/issues/1055) [#1055](https://www.github.com/googleapis/python-bigtable/issues/1055) [#1057](https://www.github.com/googleapis/python-bigtable/issues/1057) [#1093](https://www.github.com/googleapis/python-bigtable/issues/1093) [#1093](https://www.github.com/googleapis/python-bigtable/issues/1093) [#1093](https://www.github.com/googleapis/python-bigtable/issues/1093) [#1094](https://www.github.com/googleapis/python-bigtable/issues/1094) [#1094](https://www.github.com/googleapis/python-bigtable/issues/1094) [#1121](https://www.github.com/googleapis/python-bigtable/issues/1121) [#1121](https://www.github.com/googleapis/python-bigtable/issues/1121) [#1121](https://www.github.com/googleapis/python-bigtable/issues/1121) [#1156](https://www.github.com/googleapis/python-bigtable/issues/1156) [#1158](https://www.github.com/googleapis/python-bigtable/issues/1158) [#1158](https://www.github.com/googleapis/python-bigtable/issues/1158) [#1158](https://www.github.com/googleapis/python-bigtable/issues/1158) [#1186](https://www.github.com/googleapis/python-bigtable/issues/1186) [#1186](https://www.github.com/googleapis/python-bigtable/issues/1186) [#1186](https://www.github.com/googleapis/python-bigtable/issues/1186) [#1199](https://www.github.com/googleapis/python-bigtable/issues/1199) [#1199](https://www.github.com/googleapis/python-bigtable/issues/1199) [#1199](https://www.github.com/googleapis/python-bigtable/issues/1199) [#1254](https://www.github.com/googleapis/python-bigtable/issues/1254) [#1254](https://www.github.com/googleapis/python-bigtable/issues/1254) [#1254](https://www.github.com/googleapis/python-bigtable/issues/1254) [#1377](https://www.github.com/googleapis/python-bigtable/issues/1377) [#1377](https://www.github.com/googleapis/python-bigtable/issues/1377) [#1377](https://www.github.com/googleapis/python-bigtable/issues/1377) [#1441](https://www.github.com/googleapis/python-bigtable/issues/1441) [#1441](https://www.github.com/googleapis/python-bigtable/issues/1441) [#1441](https://www.github.com/googleapis/python-bigtable/issues/1441) [#1464](https://www.github.com/googleapis/python-bigtable/issues/1464) [#1464](https://www.github.com/googleapis/python-bigtable/issues/1464) [#1464](https://www.github.com/googleapis/python-bigtable/issues/1464) [#1549](https://www.github.com/googleapis/python-bigtable/issues/1549) [#1562](https://www.github.com/googleapis/python-bigtable/issues/1562) [#1555](https://www.github.com/googleapis/python-bigtable/issues/1555) [#1616](https://www.github.com/googleapis/python-bigtable/issues/1616) [#1616](https://www.github.com/googleapis/python-bigtable/issues/1616) [#1665](https://www.github.com/googleapis/python-bigtable/issues/1665) [#1670](https://www.github.com/googleapis/python-bigtable/issues/1670) [#1664](https://www.github.com/googleapis/python-bigtable/issues/1664) [#1674](https://www.github.com/googleapis/python-bigtable/issues/1674) [#1755](https://www.github.com/googleapis/python-bigtable/issues/1755) [#1755](https://www.github.com/googleapis/python-bigtable/issues/1755) [#1755](https://www.github.com/googleapis/python-bigtable/issues/1755) [#1764](https://www.github.com/googleapis/python-bigtable/issues/1764) [#1764](https://www.github.com/googleapis/python-bigtable/issues/1764) [#1770](https://www.github.com/googleapis/python-bigtable/issues/1770) [#1794](https://www.github.com/googleapis/python-bigtable/issues/1794) [#1846](https://www.github.com/googleapis/python-bigtable/issues/1846) [#1846](https://www.github.com/googleapis/python-bigtable/issues/1846) [#1846](https://www.github.com/googleapis/python-bigtable/issues/1846) [#1846](https://www.github.com/googleapis/python-bigtable/issues/1846) [#1846](https://www.github.com/googleapis/python-bigtable/issues/1846) [#1846](https://www.github.com/googleapis/python-bigtable/issues/1846) [#1878](https://www.github.com/googleapis/python-bigtable/issues/1878) [#1890](https://www.github.com/googleapis/python-bigtable/issues/1890) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#1980](https://www.github.com/googleapis/python-bigtable/issues/1980) [#2057](https://www.github.com/googleapis/python-bigtable/issues/2057) [#2057](https://www.github.com/googleapis/python-bigtable/issues/2057) [#2054](https://www.github.com/googleapis/python-bigtable/issues/2054) [#2054](https://www.github.com/googleapis/python-bigtable/issues/2054) [#2018](https://www.github.com/googleapis/python-bigtable/issues/2018) [#2018](https://www.github.com/googleapis/python-bigtable/issues/2018) [#2224](https://www.github.com/googleapis/python-bigtable/issues/2224) [#2201](https://www.github.com/googleapis/python-bigtable/issues/2201) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2436](https://www.github.com/googleapis/python-bigtable/issues/2436) [#2005](https://www.github.com/googleapis/python-bigtable/issues/2005) [#2005](https://www.github.com/googleapis/python-bigtable/issues/2005) [#2005](https://www.github.com/googleapis/python-bigtable/issues/2005) [#2005](https://www.github.com/googleapis/python-bigtable/issues/2005) [#2005](https://www.github.com/googleapis/python-bigtable/issues/2005) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#2692](https://www.github.com/googleapis/python-bigtable/issues/2692) [#3066](https://www.github.com/googleapis/python-bigtable/issues/3066) [#2707](https://www.github.com/googleapis/python-bigtable/issues/2707) [#3103](https://www.github.com/googleapis/python-bigtable/issues/3103) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#2806](https://www.github.com/googleapis/python-bigtable/issues/2806) [#3459](https://www.github.com/googleapis/python-bigtable/issues/3459) [#3494](https://www.github.com/googleapis/python-bigtable/issues/3494) [#3070](https://www.github.com/googleapis/python-bigtable/issues/3070) [#3119](https://www.github.com/googleapis/python-bigtable/issues/3119) [#3738](https://www.github.com/googleapis/python-bigtable/issues/3738) [#3738](https://www.github.com/googleapis/python-bigtable/issues/3738) [#3738](https://www.github.com/googleapis/python-bigtable/issues/3738) [#3739](https://www.github.com/googleapis/python-bigtable/issues/3739) [#3739](https://www.github.com/googleapis/python-bigtable/issues/3739) [#3740](https://www.github.com/googleapis/python-bigtable/issues/3740) [#3783](https://www.github.com/googleapis/python-bigtable/issues/3783) [#3877](https://www.github.com/googleapis/python-bigtable/issues/3877)
* **bigtable:** fix incorrect display_name update ([#46](https://www.github.com/googleapis/python-bigtable/issues/46)) ([1ac60be](https://www.github.com/googleapis/python-bigtable/commit/1ac60be05521b69c924118d40f88e07728a2f75e))
* **bigtable:** remove missing argument from instance declaration ([#47](https://www.github.com/googleapis/python-bigtable/issues/47)) ([c966647](https://www.github.com/googleapis/python-bigtable/commit/c9666475dc31d581fdac0fc1c65e75ee9e27d832)), closes [#42](https://www.github.com/googleapis/python-bigtable/issues/42)

## 1.2.1

01-03-2020 10:05 PST


### Implementation Changes
- Add ability to use single-row transactions ([#10021](https://github.com/googleapis/google-cloud-python/pull/10021))

## 1.2.0

12-04-2019 12:21 PST


### New Features
- add table level IAM policy controls ([#9877](https://github.com/googleapis/google-cloud-python/pull/9877))
- add 'client_options' / 'admin_client_options' to Client ([#9517](https://github.com/googleapis/google-cloud-python/pull/9517))

### Documentation
- change spacing in docs templates (via synth) ([#9739](https://github.com/googleapis/google-cloud-python/pull/9739))
- add python 2 sunset banner to documentation ([#9036](https://github.com/googleapis/google-cloud-python/pull/9036))

### Internal
- add trailing commas (via synth) ([#9557](https://github.com/googleapis/google-cloud-python/pull/9557))

## 1.1.0

10-15-2019 06:40 PDT


### New Features
- Add IAM Policy methods to table admin client (via synth). ([#9172](https://github.com/googleapis/google-cloud-python/pull/9172))

### Dependencies
- Pin 'google-cloud-core >= 1.0.3, < 2.0.0dev'. ([#9445](https://github.com/googleapis/google-cloud-python/pull/9445))

### Documentation
- Fix intersphinx reference to requests ([#9294](https://github.com/googleapis/google-cloud-python/pull/9294))
- Fix misspelling in docs. ([#9184](https://github.com/googleapis/google-cloud-python/pull/9184))

## 1.0.0

08-28-2019 12:49 PDT

### Implementation Changes
- Remove send/recv msg size limit (via synth). ([#8979](https://github.com/googleapis/google-cloud-python/pull/8979))

### Documentation
- Avoid creating table in 'list_tables' snippet; harden 'delete_instance' snippet. ([#8879](https://github.com/googleapis/google-cloud-python/pull/8879))
- Add retry for DeadlineExceeded to 'test_bigtable_create_table' snippet. ([#8889](https://github.com/googleapis/google-cloud-python/pull/8889))
- Remove compatability badges from READMEs. ([#9035](https://github.com/googleapis/google-cloud-python/pull/9035))

### Internal / Testing Changes
- Docs: Remove CI for gh-pages, use googleapis.dev for api_core refs. ([#9085](https://github.com/googleapis/google-cloud-python/pull/9085))

## 0.34.0

07-30-2019 10:05 PDT


### Implementation Changes
- Pick up changes to GAPIC client configuration (via synth). ([#8724](https://github.com/googleapis/google-cloud-python/pull/8724))
- Add `Cell.__repr__`. ([#8683](https://github.com/googleapis/google-cloud-python/pull/8683))
- Increase timeout for app profile update operation. ([#8417](https://github.com/googleapis/google-cloud-python/pull/8417))

### New Features
- Add methods returning Separate row types to remove confusion around return types of `row.commit`. ([#8662](https://github.com/googleapis/google-cloud-python/pull/8662))
- Add `options_` argument to clients' `get_iam_policy` (via synth). ([#8652](https://github.com/googleapis/google-cloud-python/pull/8652))
-  Add `client_options` support, update list method docstrings (via synth). ([#8500](https://github.com/googleapis/google-cloud-python/pull/8500))

### Dependencies
- Bump minimum version for google-api-core to 1.14.0. ([#8709](https://github.com/googleapis/google-cloud-python/pull/8709))
- Update pin for `grpc-google-iam-v1` to 0.12.3+. ([#8647](https://github.com/googleapis/google-cloud-python/pull/8647))
- Allow kwargs to be passed to `create_channel` (via synth). ([#8458](https://github.com/googleapis/google-cloud-python/pull/8458))
- Add `PartialRowsData.cancel`. ([#8176](https://github.com/googleapis/google-cloud-python/pull/8176))

### Documentation
- Update intersphinx mapping for requests. ([#8805](https://github.com/googleapis/google-cloud-python/pull/8805))
- Link to googleapis.dev documentation in READMEs. ([#8705](https://github.com/googleapis/google-cloud-python/pull/8705))
- Add compatibility check badges to READMEs. ([#8288](https://github.com/googleapis/google-cloud-python/pull/8288))
- Add snppets illustrating use of  application profiles. ([#7033](https://github.com/googleapis/google-cloud-python/pull/7033))

### Internal / Testing Changes
- Add nox session `docs` to remaining manual clients. ([#8478](https://github.com/googleapis/google-cloud-python/pull/8478))
- All: Add docs job to publish to googleapis.dev. ([#8464](https://github.com/googleapis/google-cloud-python/pull/8464))
- Force timeout for table creation to 90 seconds (in systests). ([#8450](https://github.com/googleapis/google-cloud-python/pull/8450))
- Plug systest / snippet instance leaks. ([#8416](https://github.com/googleapis/google-cloud-python/pull/8416))
- Declare encoding as utf-8 in pb2 files (via synth). ([#8346](https://github.com/googleapis/google-cloud-python/pull/8346))
- Add disclaimer to auto-generated template files (via synth).  ([#8308](https://github.com/googleapis/google-cloud-python/pull/8308))
- Fix coverage in `types.py` (via synth). ([#8149](https://github.com/googleapis/google-cloud-python/pull/8149))
- Integrate docstring / formatting tweaks (via synth). ([#8138](https://github.com/googleapis/google-cloud-python/pull/8138))
- Use alabaster theme everwhere. ([#8021](https://github.com/googleapis/google-cloud-python/pull/8021))

## 0.33.0

05-16-2019 11:51 PDT


### Implementation Changes
- Fix typos in deprecation warnings. ([#7858](https://github.com/googleapis/google-cloud-python/pull/7858))
- Add deprecation warnings for to-be-removed features. ([#7532](https://github.com/googleapis/google-cloud-python/pull/7532))
- Remove classifier for Python 3.4 for end-of-life. ([#7535](https://github.com/googleapis/google-cloud-python/pull/7535))
- Improve `Policy` interchange w/ JSON, gRPC payloads. ([#7378](https://github.com/googleapis/google-cloud-python/pull/7378))

### New Features
- Add support for passing `client_info` to client. ([#7876](https://github.com/googleapis/google-cloud-python/pull/7876)) and ([#7898](https://github.com/googleapis/google-cloud-python/pull/7898))
- Add `Table.mutation_timeout`, allowing override of config timeouts. ([#7424](https://github.com/googleapis/google-cloud-python/pull/7424))

### Dependencies
- Pin `google-cloud-core >= 1.0.0, < 2.0dev`. ([#7993](https://github.com/googleapis/google-cloud-python/pull/7993))

### Documentation
- Remove duplicate snippet tags for Delete cluster. ([#7860](https://github.com/googleapis/google-cloud-python/pull/7860))
- Fix rendering of instance admin snippets. ([#7797](https://github.com/googleapis/google-cloud-python/pull/7797))
- Avoid leaking instances from snippets. ([#7800](https://github.com/googleapis/google-cloud-python/pull/7800))
- Fix enum reference in documentation. ([#7724](https://github.com/googleapis/google-cloud-python/pull/7724))
- Remove duplicate snippets. ([#7528](https://github.com/googleapis/google-cloud-python/pull/7528))
- Add snippeds for Batcher, RowData, Row Operations, AppendRow. ([#7019](https://github.com/googleapis/google-cloud-python/pull/7019))
- Add column family snippets. ([#7014](https://github.com/googleapis/google-cloud-python/pull/7014))
- Add Row Set snippets. ([#7016](https://github.com/googleapis/google-cloud-python/pull/7016))
- Update client library documentation URLs. ([#7307](https://github.com/googleapis/google-cloud-python/pull/7307))
- Fix typos in Table docstrings. ([#7261](https://github.com/googleapis/google-cloud-python/pull/7261))
- Update copyright headers (via synth). ([#7139](https://github.com/googleapis/google-cloud-python/pull/7139))
- Fix linked classes in generated docstrings (via synth). ([#7060](https://github.com/googleapis/google-cloud-python/pull/7060))

### Internal / Testing Changes
- Run `instance_admin` system tests on a separate instance from `table_admin` and `data` system tests. ([#6579](https://github.com/googleapis/google-cloud-python/pull/6579))
- Re-blacken. ([#7462](https://github.com/googleapis/google-cloud-python/pull/7462))
- Copy lintified proto files (via synth). ([#7445](https://github.com/googleapis/google-cloud-python/pull/7445))
- Remove unused message exports (via synth). ([#7264](https://github.com/googleapis/google-cloud-python/pull/7264))
- Compare 0 using '!=', rather than 'is not'. ([#7312](https://github.com/googleapis/google-cloud-python/pull/7312))
- Add protos as an artifact to library ([#7205](https://github.com/googleapis/google-cloud-python/pull/7205))
- Protoc-generated serialization update. ([#7077](https://github.com/googleapis/google-cloud-python/pull/7077))
- Blacken snippets. ([#7048](https://github.com/googleapis/google-cloud-python/pull/7048))
- Bigtable client snippets ([#7020](https://github.com/googleapis/google-cloud-python/pull/7020))
- Pick up order-of-enum fix from GAPIC generator. ([#6879](https://github.com/googleapis/google-cloud-python/pull/6879))
- Plug systest instance leaks ([#7004](https://github.com/googleapis/google-cloud-python/pull/7004))

## 0.32.1

12-17-2018 16:38 PST


### Documentation
- Document Python 2 deprecation ([#6910](https://github.com/googleapis/google-cloud-python/pull/6910))
- Add snippets for table operations. ([#6484](https://github.com/googleapis/google-cloud-python/pull/6484))

## 0.32.0

12-10-2018 12:47 PST


### Implementation Changes
- Import `iam.policy` from `google.api_core`. ([#6741](https://github.com/googleapis/google-cloud-python/pull/6741))
- Remove `deepcopy` from `PartialRowData.cells` property. ([#6648](https://github.com/googleapis/google-cloud-python/pull/6648))
- Pick up fixes to GAPIC generator. ([#6630](https://github.com/googleapis/google-cloud-python/pull/6630))

### Dependencies
- Update dependency to google-cloud-core ([#6835](https://github.com/googleapis/google-cloud-python/pull/6835))

### Internal / Testing Changes
- Blacken all gen'd libs ([#6792](https://github.com/googleapis/google-cloud-python/pull/6792))
- Omit local deps ([#6701](https://github.com/googleapis/google-cloud-python/pull/6701))
- Run black at end of synth.py ([#6698](https://github.com/googleapis/google-cloud-python/pull/6698))
- Blackening Continued... ([#6667](https://github.com/googleapis/google-cloud-python/pull/6667))
- Add templates for flake8, coveragerc, noxfile, and black. ([#6642](https://github.com/googleapis/google-cloud-python/pull/6642))

## 0.31.1

11-02-2018 08:13 PDT

### Implementation Changes
- Fix anonymous usage under Bigtable emulator ([#6385](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6385))
- Support `DirectRow` without a `Table` ([#6336](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6336))
- Add retry parameter to `Table.read_rows()`. ([#6281](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6281))
- Fix `ConditionalRow` interaction with `check_and_mutate_row` ([#6296](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6296))
- Deprecate `channel` arg to `Client` ([#6279](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6279))

### Dependencies
- Update dependency: `google-api-core >= 1.4.1` ([#6391](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6391))
- Update IAM version in dependencies ([#6362](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6362))

### Documentation
- Add `docs/snippets.py` and test ([#6012](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6012))
- Normalize use of support level badges ([#6159](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6159))

### Internal / Testing Changes
- Fix client_info bug, update docstrings and timeouts. ([#6406)](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6406))
- Remove now-spurious fixup from 'synth.py'. ([#6400](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6400))
- Fix flaky systests / snippets ([#6367](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6367))
- Add explicit coverage for `row_data._retry_read_rows_exception`. ([#6364](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6364))
- Fix instance IAM test methods ([#6343](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6343))
- Fix error from new flake8 version. ([#6309](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6309))
- Use new Nox ([#6175](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6175))

## 0.31.0

### New Features
- Upgrade support level from `alpha` to `beta`.  ([#6129](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6129))

### Implementation Changes
- Improve admin operation timeouts. ([#6010](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6010))

### Documentation
- Prepare docs for repo split. ([#6014](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6014))

### Internal / Testing Changes
- Refactor `read_row` to call `read_rows` ([#6137](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6102))
- Harden instance teardown against '429 Too Many Requests'. ([#6102](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6102))
- Add `{RowSet,RowRange}.{__eq__,.__ne__}` ([#6025](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6025))
- Regenerate low-level GAPIC code ([#6036](https://github.com/GoogleCloudPlatform/google-cloud-python/pull/6036))

## 0.30.2

### New Features
- Add iam policy implementation for an instance. (#5838)

### Implementation Changes
- Fix smart retries for 'read_rows()' when reading the full table (#5966)

### Documentation
- Replace links to `/stable/` with `/latest/`. (#5901)

### Internal / Testing Changes
- Re-generate library using bigtable/synth.py (#5974)
- Refactor `read_rows` infrastructure (#5963)

## 0.30.1

### Implementation changes

- Fix non-admin access to table data. (#5875)
- Synth bigtable and bigtable admin GAPIC clients. (#5867)

### Testing and internal changes

- Nox: use in-place installs for local packages. (#5865)

## 0.30.0

### New Features

- Improve performance and capabilities of reads.  `read_rows` now returns a generator; has automatic retries; and can read an arbitrary set of keys and ranges
  - Consolidate read_rows and yield_rows (#5840)
  - Implement row set for yield_rows  (#5506)
  - Improve read rows validation performance (#5390)
  - Add retry for yield_rows (#4882)
  - Require TimestampRanges to be milliseconds granularity (#5002)
  - Provide better access to cell values (#4908)
  - Add data app profile id  (#5369)

- Improve writes: Writes are usable in Beam
  - Create MutationBatcher for bigtable (#5651)
  - Allow DirectRow to be created without a table (#5567)
  - Add data app profile id  (#5369)

- Improve table admin: Table creation now can also create families in a single RPC.  Add an `exist()` method.  Add `get_cluster_states` for information about replication
  - Add 'Table.get_cluster_states' method (#5790)
  - Optimize 'Table.exists' performance (#5749)
  - Add column creation in 'Table.create()'. (#5576)
  - Add 'Table.exists' method (#5545)
  - Add split keys on create table - v2 (#5513)
  - Avoid sharing table names across unrelated systests. (#5421)
  - Add truncate table and drop by prefix on top of GAPIC integration (#5360)

- Improve instance admin: Instance creation allows for the creation of multiple clusters.  Instance label management is now enabled.  
  - Create app_profile_object (#5782)
  - Add 'Instance.exists' method (#5802)
  - Add 'InstanceAdminClient.list_clusters' method (#5715)
  - Add 'Instance._state' property (#5736)
  - Convert 'instance.labels' to return a dictionary (#5728)
  - Reshape cluster.py, adding cluster() factory to instance.py (#5663)
  - Convert 'Instance.update' to use 'instance.partial_instance_update' API (#5643)
  - Refactor 'InstanceAdminClient.update_app_profile' to remove update_mask argument (#5684)
  - Add the ability to create an instance with multiple clusters (#5622)
  - Add 'instance_type', 'labels' to 'Instance' ctor (#5614)
  - Add optional app profile to 'Instance.table' (#5605)
  - Clean up Instance creation. (#5542)
  - Make 'InstanceAdminClient.list_instances' return actual instance objects, not protos. (#5420)
  - Add admin app profile methods on Instance (#5315)

### Internal / Testing Changes
- Rename releases to changelog and include from CHANGELOG.md (#5191)
- Fix bad trove classifier
- Integrate new generated low-level client (#5178)
- Override gRPC max message lengths. (#5498)
- Use client properties rather than private attrs (#5398)
- Fix the broken Bigtable system test. (#5607)
- Fix Py3 breakage in new system test. (#5474)
- Modify system test for new GAPIC code (#5302)
- Add Test runs for Python 3.7 and remove 3.4 (#5295)
- Disable Bigtable system tests (#5381)
- Modify system tests to use prerelease versions of grpcio (#5304)
- Pass through 'session.posargs' when running Bigtable system tests. (#5418)
- Harden 'test_list_instances' against simultaneous test runs. (#5476)
- Shorten instance / cluster name to fix CI breakage. (#5641)
- Fix failing systest: 'test_create_instance_w_two_clusters'. (#5836)
- Add labels {'python-system': ISO-timestamp} to systest instances (#5729)
- Shorten cluster ID in system test (#5719)
- Harden 'test_list_instances' further. (#5696)
- Improve testing of create instance (#5544)

## 0.29.0

### New features

- Use `api_core.retry` for `mutate_row` (#4665, #4341)
- Added a row generator on a table. (#4679)

### Implementation changes

- Remove gax usage from BigTable (#4873)
- BigTable: Cell.from_pb() performance improvement (#4745)

### Dependencies

- Update dependency range for api-core to include v1.0.0 releases (#4944)

### Documentation

- Minor typo (#4758)
- Row filter end points documentation error (#4667)
- Removing "rename" from bigtable table.py comments (#4526)
- Small docs/hygiene tweaks after #4256. (#4333)

### Testing and internal changes

- Install local dependencies when running lint (#4936)
- Re-enable lint for tests, remove usage of pylint (#4921)
- Normalize all setup.py files (#4909)
- Timestamp system test fix (#4765)

## 0.28.1

### Implementation Changes

- Bugfix: Distinguish between an unset column qualifier and an empty string
  column qualifier while parsing a `ReadRows` response (#4252)

### Features added

- Add a ``retry`` strategy that will be used for retry-able errors
  in ``Table.mutate_rows``. This will be used for gRPC errors of type
  ``ABORTED``, ``DEADLINE_EXCEEDED`` and ``SERVICE_UNAVAILABLE``. (#4256)

PyPI: https://pypi.org/project/google-cloud-bigtable/0.28.1/

## 0.28.0

### Documentation

- Fixed referenced types in `Table.row` docstring (#3934, h/t to
  @MichaelTamm)
- Added link to "Python Development Environment Setup Guide" in
  project README (#4187, h/t to @michaelawyu)

### Dependencies

- Upgrading to `google-cloud-core >= 0.28.0` and adding dependency
  on `google-api-core` (#4221, #4280)

PyPI: https://pypi.org/project/google-cloud-bigtable/0.28.0/
