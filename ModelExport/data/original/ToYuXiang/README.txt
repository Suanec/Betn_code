各文件说明：

conf/save.model.20160927为模型文件

conf/feature.conf为feature.map文件。（fea.map文件"_other_"维度目前未使用。）

KVData/abnormal_sampleKV.rst为异常用户源数据KeyValue格式文件。其中数据为源数据，添加了对应特征名称。

KVData/normal_sampleKV.rst为正常用户源数据KeyValue格式文件。

KVData/sampleKV.rst为正常用户与异常用户联合后的全量数据，格式为KeyValue。

rawData/abnormal_sample.txt为异常数据源文件，目前测试数据集最原始格式。已添加表头行。当前对应类标0。

rawData/normal_sample.txt为正常用户源数据文件。已添加表头行。当前对应类标1。

feature_map_flow.rst为特征映射流程说明文件，包括特征映射操作和模型预测操作。(若有表述不清楚的地方，敬请指正。)

feature_Meaning.rst为特征含义说明文件，包括了顺序与源数据相同，包括对应维度实际含义与特征名称。

模型预测计算示意图.jpg为模型预测所做线性运算示意图，便于理解预测步骤的具体操作。来源自9月29日周会分享PPT。

（此上，如有遗漏，请提出。如有谬误，请指正。如有不解，相互讨论，共同进步！祝好。）



