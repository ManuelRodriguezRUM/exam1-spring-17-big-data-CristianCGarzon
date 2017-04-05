#Code here
schoolsOriginal = sc.textFile('/cristian/examBigData/hive/escuelasPR.csv')
schoolsCsv = schoolsOriginal.map(lambda line : line.split(','))
result = schoolsCsv.filter( lambda city: city[2] == 'Ponce' or city[2] == 'Cabo Rojo' or city[2] == 'Dorado')
result.collect()