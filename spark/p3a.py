#Code here
studentsOriginal = sc.textFile('/cristian/examBigData/hive/studentsPR.csv')
studCsv = studentsOriginal.map(lambda line : line.split(','))
result = studCsv.filter(lambda sex : sex[5] == 'F').filter(lambda code : code[2] == '71381')
result.collect()