
import jieba
import re
import os
# 此处使用snownlp模块对评论进行情感分析
from snownlp import SnowNLP
from JdXmSpy import  *   #导入 JdXmSpy文件

sqldb= r'''	select comment,id from phone  group by comment,id
    having len(comment)<5 and id  in  (select id  from phone where xh like '%小米8%') 
	and comment in (select words from keys)
	union
	select comment, max(id) as id from phone 
	group by comment having   comment  in  (select comment  from phone where xh like '%小米8%') and len(comment)>=5 
	union
	select comment, max(id) as id from phone 
	group by comment having   comment  in  (select comment  from phone where xh like '%小米8%') and
	len(comment)<5 and comment not in (select words from keys)
   '''

sqldbg='''

	select comment,id from phone  group by comment,id
    having id  in  (select id  from phone where  len(comment)<5 and  comment in (select words from keys)  and  xh like '%小米8%') 
	union
	select comment, max(id) as id from phone 
	group by comment having  comment  in  (select comment  from phone where  len(comment)>=5 and xh like '%小米8%' ) 
	union
	select comment, max(id) as id from phone 
	group by comment having   comment  in  (select comment  from phone where  len(comment)<5 and comment not in (select words from keys) and xh like '%小米8%' ) 
	
'''

advself= ['很', '级', '特', '非', '超', '贼', '太','极']  #自定义的最常见程度副词词典,其权重大于默认的程度副词
eng = ['好', '比', '很', '爱', '赞', '喜', '不', '可', '非', '棒', '大', '评', '快', '正', '差', '卡', '有', '满', '超', '值', '还', '贼',
       '挺', '太', '蛮', '五星'
                      '舒服']  # 停用词忽略的,

createTb=r''' create table commentmark(
	id char(32) primary key,
	comment char(5012),
	snowNlp float,
	 mark int
	)
	'''

insertCommentmark="insert commark values(%s,%s,%s,%s)"

class DataProc():
    """
    数据处理类
    数据爬虫采集的评论数据。
    """

    def __init__(self):
        self.xm=TbXmSpy("main",None,ip_dict[0])
        db=self.xm.dbProc() #打开数据库链接
        self.conn=db[0]
        self.cursor=db[1]
        self.count=0

    def createTable(self,sql): #创建表格
        self.xm.createTable(sql)

    def insertDate(self,sql,data):
        self.xm.insertData(None,sql,data)

    def writeCsv(self,sql,path,rst):
        with open(path, 'a') as f:  # 追加写话
            write = csv.writer(f)
            write.writerow(("id","评价","几星","snlp计算","权重","手工标注"))
            for rs in rst:
                data = (rs[0], rs[1], rs[2], rs[3], rs[4], 1)
                write.writerow(data)
            f.close()

    def getZipStr(self,scent):  # 压缩了句首
        temp1 = scent.strip('\n')  # 去掉每行最后的换行符'\n'
        temp2 = temp1.lstrip('\ufeff')
        temp3 = temp2.strip('\r')
        char_list = list(temp3)
        list1 = ['']
        list2 = ['']
        del1 = []
        flag = ['']
        i = 0
        while (i < len(char_list)):
            if (char_list[i] == list1[0]):
                if (list2 == ['']):
                    list2[0] = char_list[i]
                else:
                    if (list1 == list2):
                        t = len(list1)
                        m = 0
                        while (m < t):
                            del1.append(i - m - 1)
                            m = m + 1
                        list2 = ['']
                        list2[0] = char_list[i]
                    else:
                        list1 = ['']
                        list2 = ['']
                        flag = ['']
                        list1[0] = char_list[i]
                        flag[0] = i
            else:
                if (list1 == list2) and (list1 != ['']) and (list2 != ['']):
                    if len(list1) >= 2:
                        t = len(list1)
                        m = 0
                        while (m < t):
                            del1.append(i - m - 1)
                            m = m + 1
                        list1 = ['']
                        list2 = ['']
                        list1[0] = char_list[i]
                        flag[0] = i
                else:
                    if (list2 == ['']):
                        if (list1 == ['']):
                            list1[0] = char_list[i]
                            flag[0] = i
                        else:
                            list1.append(char_list[i])
                            flag.append(i)
                    else:
                        list2.append(char_list[i])
            i = i + 1
            if (i == len(char_list)):
                if (list1 == list2):
                    t = len(list1)
                    m = 0
                    while (m < t):
                        del1.append(i - m - 1)
                        m = m + 1
                    m = 0
                    while (m < t):
                        del1.append(flag[m])
                        m = m + 1
        a = sorted(del1)
        t = len(a) - 1
        while (t >= 0):
            # print(char_list[a[t]])
            del char_list[a[t]]
            t = t - 1
        str1 = "".join(char_list)
        return str1.strip()  # 删除两边空格

    def selectComm(self,sql,cond):
        if cond:
            self.cursor.execute(sql, cond)
        else:
            self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def getDict(self,path):   #加载自定义词典，还回列表,支持utf-8，以及gbk编码
        try:
            words = [i.strip() for i in open(path, 'r', encoding='UTF-8-sig').readlines()]
            return words
        except Exception as e:
            try:
                words = [i.strip() for i in open(path, 'r', encoding='gbk').readlines()]
                return  words
            except Exception as e:
                print("读文件错误",e)


    def insert(self,sql,data):
        try:
            self.cursor.execute(sql, data)
            self.count += 1
            if not self.count%6:  #3条已提交
                self.conn.commit()
                print("插入", self.count, "成功")
        except Exception as e:
            print("插入数据发生问题", e)
            self.conn.rollback()


    def stopWord(self,path):   #停用词表，需要忽略掉程度副词词典,否定词典,消极词典里面的词语，以及自定义的词
        podict=self.getDict("dict//advdict.txt")  #程度副词典
        advdict=self.getDict("dict//nodict.txt")  #否定词典
        negdict=self.getDict("dict//negdict.txt")

        wordlist=[]
        for i in open(path, 'r', encoding='utf-8').readlines():
            w=i.strip()  #去掉空格
            for en in eng:
                n=w.find(en)
                if n>=0:
                    break
            if n>=0:
                continue
            if w in podict:
                continue
            if w in advdict:
                continue
            if w in negdict:
                continue
            wordlist.append(w)
        # self.writeDict("dict\\stop.txt",wordlist)  #处理掉的停用词表
        return  wordlist

    def deleStopWord(self,scenlist,stopwords ):
        # stopwords = dbp.stopWord("dict\\stopword.txt")  # 去停用词
        sent=[]
        for word in scenlist:
            if word in stopwords:
                continue
            else:
                sent.append(word)
        return sent

    def writeDict(self,path,wordlist):  #写字典
        print("写入字词的个数 ",len(wordlist))
        with open(path, 'a',encoding='utf-8') as f:  # 追加写入
            for word in wordlist:
                try:
                    f.write(word+"\n")
                except Exception as e:
                    print("编码异常",e)
                    continue
            f.close()

    def getFileNameList(self,root):  # 换回root文件目录下的文件名
        filename =[]
        for  file in os.listdir(root):
            pathname=os.path.join(root,file)  #避免相对路径的问题
            if os.path.isfile(pathname):
                filename.append(pathname)
        return  filename





    def getUserDict(self,userpath,dirpath):  #目录下的文件合并的字典

        filenames= self.getFileNameList(dirpath)
        name=[]
        for fn in filenames:
            name.append(self.getDict(fn))
        wordlist = []
        wordset = set()
        count = 0
        n = 0
        for dt in name:
            leng = len(dt)
            for i in dt:
                word = re.sub("[a-zA-Z0-9\!\%\,\.\']", "", i).strip().strip("\r\n")
                # print(word)
                wordset.add(word)
                # wordset.add(i)
                count += 1
        wordlist = list(wordset)
        self.writeDict(userpath,wordlist)

    def getSnowmark(self,comments):
        s = SnowNLP(comments)
        return s.sentiments

    def getMark(self,rs,posdict,negdict,advdict,nodict,poscomment,negcomment):

        jieba.load_userdict("dict\\userdict.txt")  # 自定义词典
        short = self.getDict("dict\\short.txt")  # 列表形式的不过滤表
        stopwords = dbp.stopWord("dict\\stopword.txt")  # 去停用词
        for row in rs:   #遍历数据库的列   row[0]评论， row[1] 用户的id
            q = 0  # 权重
            comment=row[0].strip().replace('#','').replace('、','') .replace('？','').replace(',','').replace('；','').replace('~','')  #清除无效字符，停用词表也有
            rbcomm=self.getZipStr(comment)  #机械压缩
            if len(rbcomm)==0 :
                continue
            if  rbcomm not in short and len(rbcomm)<5: #长度小于5，不在过滤表中忽略数据。
                continue
            scet=list(jieba.cut(rbcomm))
            descet=self.deleStopWord(scet,stopwords)  #去掉停用词
            for i  in range(len(descet)):
                if  descet[i] in posdict:   #在积极情感词典中
                    if i>0 and descet[i-1] in  advdict:
                            q+=2
                    elif i>0 and descet[i-1] in nodict:
                        q-=1
                    elif i>0 and descet[i] in negdict:
                        q-=1
                    elif i < len(descet) - 1 and descet[i + 1] in negdict:
                        q-=1
                    else: q+=1
                elif  i>0 and descet[i] in negdict:
                    if i > 0 and descet[i - 1] in advdict:
                        q -= 2
                    elif i > 0 and descet[i - 1] in nodict:
                        q += 1
                    elif i>0 and descet[i-1] in negdict:
                        q+=1
                    else:
                        q-=1
                elif descet[i] in nodict:  #前面没有词典副词和其词性相反的词
                    q-=0.5
                elif descet[i] in poscomment: #在正面评论词典
                    q+=0.5
                elif descet[i] in negcomment: #在负面评论词典
                    q-=0.5
                print("q=",q)
            snowmark=self.getSnowmark(row[0])
            print( snowmark,q)
            data=(row[1],row[0],snowmark,q)
            self.insert(insertCommentmark,data)





if __name__ == '__main__':
    dbp=DataProc()
    posdict=dbp.getDict("dict\\posdict.txt") #正面情感词
    negdict=dbp.getDict("dict\\negdict.txt") #负面情感词
    advdict=dbp.getDict("dict\\advdict.txt")  #长度副词
    nodict=dbp.getDict("dict\\nodict.txt")  #否定词
    poscomment=dbp.getDict("dict\\poscomment.txt") #正面评论
    negcomment=dbp.getDict("dict\\negcomment.txt") #负面评论

    ransql='select  top 1000 commark.id,commark.comment,score,snowNlp,commark.mark from commark ,phone where commark.id=phone.id'

    t1=time.time()
    # rs=dbp.selectComm(sqldbg,None)
    rs=[]
    row1=[" 好东西靠谱，送的东西也好，一直喜欢华为手机      ","3333" ]
    row2=["  很快，很强，很牛逼，","6969"]
    row3=[" 手机到了，好用的不得了。点赞点赞点赞，下次还买！      ","8899"]
    row4=["本来担心翻车，事实证明，没有翻车，正品。","8999"]
    rs.append(row1)
    rs.append(row2)
    rs.append(row3)
    # rs.append(row4)

    dbp.getMark(rs,posdict,negdict,advdict,nodict,poscomment,negcomment)
    print("开始的时间为: ",time.strftime('%Y.%m.%d.%H.%M.%S',time.localtime(t1)))
    rst=dbp.selectComm(ransql,None)

    # dbp.writeCsv(ransql,"dict\\end.csv",rst)

    t2=time.time()

    print("结束的时间为: ", time.strftime('%Y.%m.%d.%H.%M.%S', time.localtime(t2)))
    userpath = "dict\\stopword.txt"
    dirpath="dict\\stopwords"
    # dbp.getUserDict(userpath,dirpath)





    # userpath = "dict//stopword.txt"
    # dirpath="dict\\stopwords"
    # dbp.getUserDict(userpath,dirpath)
    # dbp.getUserDict()
    # dbp.createTable(createTb)
    # pathShort="D:\\short.txt"
    # shortSql="insert keys values(%s)"
    # shortlist=dbp.getDict(pathShort)
    # for shw in shortlist:
    #     wd=''.join(tuple(shw))
    #     print(wd)
    #    dbp.insert(shortSql,wd)





