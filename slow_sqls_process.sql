update default.slow_sqls set enabled = false
where hash_id in (
'4c104c44a675a08b862726f7af743405',
'9f3f6d85e3bfc73be0f1e23cec87fd7e',
'81335bbe700988babfe7fe1a8f9d95b5',
'3b432935394ed14b0477650ade33a83a',
'2fdcff6e85e162508e5385057049da3f',
'4ea779bfe4da44953b696d163d29181f',
'58537fc4cace8ed1a6949ff62332b25e',
'65855c658b34ce323a73ca64482403d8',
'8d09e93d37a7385b8ed724125410067e',
'8f42ae6954be3b9e97ed78a1291182ef',
'5aaf47d6046ff88e878c60ff72038643',
'67fbe38d7166fc06890ee0b06e314d43',
'2f7cf96a403d67a5aa8662a11606fafc',
'9b767adf91ba196c17874fdfb32fdd7d',
'b9ee30af906d6ef93bb7d0f8889cd4e0',
'83cb94cc344fb5bd6c7290869cc4e200',
'23ce683cdffa7608c4801463ebb42220',
'fe6297d31c624454afc1e7cd8dcf6007',
'59b1a766540a3dcae392e86c21806147',
'c37c4c6017dc5c4a6242506e0003ae70',
'511427f9b7581a7a381477347a6d28e1',
'a4f9f5a9d14560c1584f6a28abf0d439',
'7dea9d6b462ad7d9ed63df563534cdb7',
'7d0589e240eb378ea4e6475d74b2adcc',
'e2348d450fd83c07706c76f2f0badc49',
'420daf90404544453fcc506db13a6496',
'023d0f145e748b4141efd05b4fec3efb',
'094654b7380d8cf55805446c3cd1bb10',
'783fc3285d3c79ee023299d33410ef51',
'df44d8152bd85459b92a0ae4e4855f37',
'4ef005ba494e816480fdefb62b1e7936',
'535203505c43973e71f5db10fa2059b1',
'9355e277df6c436aef31f24f41d06453',
'cf2d61716de59cca22562e0801c38c8c',
'd126528be5d87363fb6e490d148f3d22',
'41f664cea4bb0efe4c3156c88ff52929',
'6435f0aedd5be36f1c53da636cba1f4d',
'80c4d9a165d6d975e3c8b1af12ce8644',
'fef83e670d3e1e8542e63cbe235e4a41',
'137a90a512becf6c3655b2ff4bcaaf3d',
'33dcf3b696d9ae015bbcc951f5f8421b',
'b3d168e0efbbcc40e80bbefc840abbc6',
'2751ea451d72bac81baa66b9c033c3da',
'e8d732257faf9f16da76f2662c4e5145',
'2db43edb1d3f3e6593d1b765fbf14800',
'3d9edeff08be8c32e49391567999fe34',
'10e581ad09509206222b82eab8ae578a',
'51f9a976d35a6e893ed58d4d7badf72a',
'2896a8a027d25e9eb1c31b97f6f3e0d3',
'130b7410b738deeee9c753bc9f09cabb',
'45b076db3e08528657fff5085379dd6c',
'5dd7e8ec687f05458b3dbf0ca1ede767',
'203cd735ab47b11d2a154de0067655ff',
'8e0a9a5b4fb5ceb204a9f9cfa27fdecc',
'a235d43f810f4e6d692301f9c29220b4'
)
;
-- 处理tidb
UPDATE test.`slow_sqls`
SET sql_tidb = REPLACE(sql_tidb, 'NULLS FIRST', '')
WHERE sql_tidb LIKE '%NULLS FIRST%'
;
UPDATE test.`slow_sqls`
SET sql_tidb = REPLACE(sql_tidb, 'isnull(', 'ifnull(')
WHERE sql_tidb LIKE '%isnull(%'
;
UPDATE test.`slow_sqls`
SET sql_tidb = REPLACE(sql_tidb, 'isNull(', 'ifnull(')
WHERE sql_tidb LIKE '%isNull(%'
;
UPDATE test.`slow_sqls`
SET sql_tidb = REPLACE(sql_tidb, '--', '-- ')
WHERE sql_tidb LIKE '%--%'
;
UPDATE test.`slow_sqls` 
SET db = 'global_platform'
WHERE db = 'default'
;
UPDATE test.`slow_sqls` 
SET sql_tidb = REPLACE(sql_tidb, 'global_dw.', 'global_dw_1.')
WHERE sql_tidb LIKE '%global_dw.%'
;
UPDATE test.`slow_sqls` 
SET sql_tidb = REPLACE(sql_tidb, 'cast ', 'cast')
WHERE sql_tidb LIKE '%cast %'
;
UPDATE test.`slow_sqls`
SET sql_tidb = REPLACE(sql_tidb, 'nullvalue(', 'isnull(')
WHERE sql_tidb LIKE '%nullvalue(%'