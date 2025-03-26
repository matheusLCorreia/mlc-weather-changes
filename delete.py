import psycopg2

data = [
	{
		"municipio_id" : 3503109,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3503208,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500204,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3500808,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3501301,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3501905,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3504503,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3505104,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3505302,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3505807,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3506201,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3506359,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3507704,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3507753,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3507902,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3508504,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3509106,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3509205,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3509908,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3510104,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3512308,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3512407,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3513009,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3513108,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3513504,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3513603,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3513900,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3514007,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3515202,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3516200,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3519204,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3519402,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3519709,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3519808,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3520103,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3520202,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3520301,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3520426,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3520707,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3520806,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3522406,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3522505,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3523008,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3524204,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3524501,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3526407,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3526803,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3527256,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3527306,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3528007,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3528502,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3528700,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3528809,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3500105,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3500303,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3500501,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500550,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500105,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3500303,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3500501,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3500550,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3500808,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3501202,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3501400,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3501608,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3500709,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3500907,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500907,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3500600,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3500758,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3501202,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3501152,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502309,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3502507,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3502705,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3501707,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502705,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3502804,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502903,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3503000,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3503307,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3500600,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3500758,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3501707,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3502002,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3502408,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3503950,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3504107,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3504305,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3504602,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3504800,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3505005,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3505351,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3506102,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3508108,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3508405,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3508702,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3508801,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3508900,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3509007,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3509452,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3509700,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3510005,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3511607,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3511904,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3512001,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3512506,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3512902,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3513207,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3513306,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3515194,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3515350,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3515608,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3515806,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3516309,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3516606,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3516853,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3516903,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3517000,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3517109,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3517307,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3519055,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3519253,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3519600,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3520004,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3521002,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3523206,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3523800,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3524006,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3524600,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3526506,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3527504,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3527702,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3528858,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3500204,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3500402,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500402,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3500105,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3500303,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3501152,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3501509,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3501103,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500600,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3500758,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3501103,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3501806,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3502101,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3502200,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3501152,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3501202,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502606,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3501905,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502804,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3502903,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3503000,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3502606,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3503158,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500501,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3500550,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3500709,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3500907,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3501202,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3501400,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3501608,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3503802,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3503901,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3504206,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3505708,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3506003,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3506300,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3507803,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3508603,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3509254,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3509304,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3509601,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3509809,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3509957,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3512605,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3512803,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3513702,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3513801,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3557303,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3515509,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3515657,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3516002,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3516507,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3516804,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3517208,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3518909,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3519303,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3519501,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3520509,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3520905,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3522604,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3522653,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3522703,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3522901,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3523404,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3523602,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3523909,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3524709,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3524808,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3526605,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3526704,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3527207,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3527405,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3527603,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3528106,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3528205,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3528403,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3528601,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3500501,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3500550,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3500105,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3500303,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3500204,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3500907,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3501301,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3500808,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3500709,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3500808,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3501905,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3501301,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3501400,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3501608,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3502408,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3501806,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3500105,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3500303,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3500402,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3501103,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3501152,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3501509,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3501806,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3502101,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3502200,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3502309,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502507,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3504008,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3504404,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3504701,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3504909,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3505203,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3505609,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3505906,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3508009,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3508207,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3508306,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3511706,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3557204,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3512100,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3512209,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3512704,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3513405,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3513850,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3514106,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3515301,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3515400,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3515707,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3515905,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3516101,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3516408,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3516705,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3518859,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3519006,
		"date_time" : 1293847200
	},
	{
		"municipio_id" : 3519105,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3519907,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3520442,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3520400,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3520608,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3522802,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3523107,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3523305,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3523503,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3523701,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3524105,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3524303,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3524402,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3524907,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3526902,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3527009,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3527108,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3527801,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3527900,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3528304,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3500204,
		"date_time" : 1357005600
	},
	{
		"municipio_id" : 3500402,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3500204,
		"date_time" : 1483236000
	},
	{
		"municipio_id" : 3501103,
		"date_time" : 1325383200
	},
	{
		"municipio_id" : 3500600,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3500758,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3500709,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3501707,
		"date_time" : 1388541600
	},
	{
		"municipio_id" : 3502002,
		"date_time" : 1420077600
	},
	{
		"municipio_id" : 3501509,
		"date_time" : 1514772000
	},
	{
		"municipio_id" : 3501301,
		"date_time" : 1546308000
	},
	{
		"municipio_id" : 3502754,
		"date_time" : 1451613600
	},
	{
		"municipio_id" : 3502754,
		"date_time" : 1514772000
	}
]

def connectDatabase():
    try:
        with psycopg2.connect(f"""host=localhost dbname=weather_db port=5432 user=postgres password=18223005""") as conn:
            cur = conn.cursor()
            return conn, cur
    except (psycopg2.Error, psycopg2.DatabaseError) as pg:
        print(pg)
        exit(1)
    return None, None

conn, cur = connectDatabase()
for row in data:
    cur.execute(f"delete from tbl_open_weather_hist where municipio_id = {row['municipio_id']} and date_time = {row['date_time']};")
    conn.commit()
    
cur.close()
conn.close()
    