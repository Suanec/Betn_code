特征映射：
1、删除第1、2列，即uid与hour。
2、对剩下17列做max20操作，具体为观测当前维度数值，大于20的值一律按照20处理。
3、将得到的max20结果数据做one-hot-encoding，即一维特征扩展为21维分别对应0~20，当前维度数值决定其位置。
  （若使用feature.map进行构建，可直接按照特征名与对应维度映射。需要注意的是为与LR模型兼容，引入22维特征，多余特征为"_other_"，当前逻辑中该维度未使用。）维度校验：17*21 = 357,357为模型接受样本维度。


模型训练与预测：
1、当前模型文件中，类标0对应非正常数据，类标1对应正常数据。
2、模型文件中pi为类别先验概率log值，即log(p(yi))。需要注意的模型文件中保存的数值为log计算结果。
  (pi log of class priors, whose dimension is C, number of labels)
3、模型文件中theta表示训练结束得到的后验概率log值，即log(p(xi|yi)), 计算公式为: argmax(theta * testData + pi)。其中运算符均为矩阵操作。
  spark计算逻辑：
    prob = thetaMatrix.multiply(testData)
    prob += piVector.multiply(1.0)
    labels(prob.argmax)

  (theta log of class conditional probabilities, whose dimension is C-by-D,where D is number of features)

4、值得注意的是关于朴素贝叶斯的平滑因子，因为其仅作为模型训练防止数据异常使用，在模型预测过程中并未使用，故模型文件中未对其进行记录。可忽略之。