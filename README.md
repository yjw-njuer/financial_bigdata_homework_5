# financial_bigdata_homework_5
<h1>金融大数据作业5</h1><br>
<h2>221275029_杨珺雯</h2><br>
<h2>一、设计思路</h2><br>
<h3>1.统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从⼤到⼩输出，输出格式为“<排名>：<股票代码>，<次数>”；</h3><br>
设计三个类：<strong><em>StockDriver、StockMapper、StockReducer</em></strong>。<br>
<strong><em>StockDriver</em></strong>类中读取文件、接收StockMapper和StockReducer的处理结果，设置输入输出格式和路径。<br>
<strong><em>StockMapper</em></strong>读取文件中的信息，把文件中的内容按照逗号分隔开成为单词，存入数组中；定义一个函数，用于判断每个单词是否由数字构成，由数字构成的即为股票代码，计数1存入context。<br>
<strong><em>StockReducer</em></strong>收到StockMapper的信息，遍历每个股票代码的计数，对相同的股票代码进行汇总，将其相加并将总计数存入stockCountMap；然后定义一个排序的函数，将stockCountMap的条目转换为列表并按出现次数降序排序，遍历排序后的列表，将排名和股票代码及其出现次数输出。<br>

<h3>2.统计数据集热点新闻标题（“headline”列）中出现的前100个⾼频单词，按出现次数从⼤到⼩输出。要求忽略⼤⼩写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为“<排名>：<单词>，<次数>”。 </h3><br>
设计三个类：<strong><em>NewsMapper、NewsReducer、NewsAnalysis</em></strong>。<br>
<strong><em>NewsAnalysis</em></strong>创建作业实例，使用NewsMapper和NewsReducer类，设置输入输出格式和路径。<br>
<strong><em>NewsMapper</em></strong>类中，先设置一个setup函数，用于从Hadoop配置中获取停用词文件的路径，然后使用FileSystem读取文件内容，并将每个非空的停用词添加到stopWords集合中，停用词统一转为小写，确保比较时不区分大小写；然后在map方法中检查输入的value是否为null或空、将输入行按逗号分割成字段（fields）、从第二列到倒数第二列拼接形成标题、将标题转换为小写并按非字母字符分割成单词，逐个检查每个单词，确保它不在停用词集合中，且不为空，最后使用isValidWord方法验证有效性（只允许字母和连字符，不允许数字和特殊字符），有效的单词通过context.write输出，计数为1；最后编写isValidWord函数，检查单词是否只包含字母和连字符，并且长度大于1。<br>
<strong><em>NewsReducer</em></strong>类中如果 countMap 的大小超过 100，则移除出现次数最少的单词（使用 firstKey() 方法获取最小的键），计算出所有出现次数的总和，并将单词和其总次数存入 countMap；然后设计排序函数cleanup，将降序的单词和出现次数按格式给出。<br>

<h2>二、程序运行结果</h2><br>
<h3>1.统计股票代码</h3><br>
<strong><em>output结果</em></strong>：输出按出现个数降序排序的股票代码和出现次数<br>![屏幕截图 2024-10-24 135459](https://github.com/user-attachments/assets/21574a9d-0e20-4fa6-9649-b9fb842a622b)<br>
<strong><em>web页面截图</em></strong>：作业运行成功<br>![屏幕截图 2024-10-24 142500](https://github.com/user-attachments/assets/d58deb4a-9da8-444a-9bd1-d3255621383d)<br>

<h3>2.统计新闻标题</h3><br>
<strong><em>output结果</em></strong>：输出按出现个数降序排序的标题中的单词<br>![屏幕截图 2024-10-24 214605](https://github.com/user-attachments/assets/8a6437a6-f566-4963-9efa-caf787882a8e)<br>
<strong><em>web页面截图</em></strong>：作业运行成功<br>![屏幕截图 2024-10-24 142500](https://github.com/user-attachments/assets/3f620447-5112-46e0-a1cd-072b19be0abd)<br>

<h2>三、不足与可改进之处</h2><br>
<h3>1.硬编码的参数数量和类型</h3>：代码要求严格的命令行参数数量（3个），如果未来需要添加更多的配置选项（如额外的输入文件、输出格式等），会导致代码变得臃肿和难以维护。可以通过配置文件来设置参数，减少命令行参数的数量，提高灵活性。<br>
<h3>2.停用词文件的单一性</h3>：目前只支持一个停用词文件，限制了读取多个文件的灵活性，很不方便。下次可以研究同时读取多个文件的方法。<br>
