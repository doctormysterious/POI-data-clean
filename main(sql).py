# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from io import StringIO
import pandas as pd
import re
import os

def read_sql_script_text(sql_file_path):
  file=open(sql_file_path,'r',encoding='utf-8')
  file_data=file.readlines()
  for row in file_data:
    tmp_list=row.split(' ')
    type_name=tmp_list[6].replace("'",'').replace(",",'')
    city_name=tmp_list[17].replace("'",'').replace(",",'')


def read_sql_script_all(sql_file_path, quotechar="'") -> (str, dict):
  print("test")
  insert_check = re.compile(r"`东北`", re.I | re.A)
  with open(sql_file_path, encoding="utf-8") as f:
    sql_txt = f.read()
  end_pos = -1
  df_dict = {}
  while True:
    match_obj = insert_check.search(sql_txt, end_pos+1)
    if not match_obj:
      break
    table_name = match_obj.group(0)
    print(table_name)
    start_pos = match_obj.span()[1]+1
    end_pos = sql_txt.find(";", start_pos)
    tmp = re.sub(r"( VALUES |,)\(", "\n", sql_txt[start_pos:end_pos])
    df = pd.read_csv(StringIO(tmp), quotechar=quotechar)
    dfs = df_dict.setdefault(table_name, [])
    dfs.append(df)
  for table_name, dfs in df_dict.items():
    df_dict[table_name] = pd.concat(dfs)
  return df_dict

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    #酒水类poi
def CalAllCityPosOnlyType():
  # city_name_list = ['北京市', '上海市', '天津市', '重庆市', '长春市', '沈阳市', '哈尔滨市', '呼和浩特市', '石家庄市', '乌鲁木齐市', '兰州市', '西宁市', '西安市',
  #                   '银川市', '郑州市',
  #                   '济南市', '太原市', '合肥市', '长沙市', '武汉市', '南京市', '成都市', '贵阳市', '昆明市', '南宁市', '拉萨市', '杭州市', '南昌市', '广州市',
  #                   '福州市', '海口市', '台北市', '香港', '澳门']
  city_name_list = ['石河子市', '吐鲁番市', '喀什地区', '和田地区', '五家渠市', '阿拉尔市', '图木舒克市', '亳州市', '巢湖市', '万宁市', '东方市', '五指山市', '文昌市',
                    '陵水黎族自治县', '澄迈县', '乐东黎族自治县', '临高县', '定安县', '昌江黎族自治县', '屯昌县', '保亭黎族苗族自治县', '白沙黎族自治县',
                    '琼中黎族苗族自治县']
  # city_name_list = ['北京市', '上海市', '天津市', '重庆市', '广州市', '深圳市', '珠海市', '汕头市', '佛山市', '韶关市', '河源市', '梅州市', '惠州市', '汕尾市',
  #                   '东莞市', '中山市', '江门市', '阳江市', '湛江市', '茂名市', '肇庆市', '清远市', '潮州市', '揭阳市', '云浮市', '合肥市', '淮北市', '毫州市',
  #                   '宿州市', '蚌埠市', '阜阳市', '淮南市', '滁州市', '马鞍山市', '芜湖市', '宣城市', '铜陵市', '安庆市', '六安市', '黄山市', '池州市', '巢湖市',
  #                   '福州市', '莆田市', '三明市', '龙岩市', '厦门市', '泉州市', '漳州市', '宁德市', '南平市', '南宁市', '柳州市', '桂林市', '贺州市', '贵港市',
  #                   '玉林市', '河池市', '北海市', '钦州市', '防城港市', '百色市', '梧州市', '来宾市', '崇左市', '兰州市', '庆阳市', '定西市', '武威市', '酒泉市',
  #                   '张掖市', '嘉峪关市', '平凉市', '天水市', '白银市', '金昌市', '陇南市', '临夏回族自治州', '甘南藏族自治州', '贵阳市', '黔南布依族苗族自治州', '六盘水市',
  #                   '遵义市', '黔东南苗族侗族自治州', '铜仁市', '安顺市', '毕节市', '黔西南布依族苗族自治州', '石家庄市', '衡水市', '张家口市', '承德市', '秦皇岛市',
  #                   '廊坊市', '沧州市', '保定市', '唐山市', '邯郸市', '邢台市', '哈尔滨市', '大庆市', '伊春市', '大兴安岭地区', '黑河市', '鹤岗市', '七台河市',
  #                   '齐齐哈尔市', '佳木斯市', '牡丹江市', '鸡西市', '绥化市', '双鸭山市', '郑州市', '南阳市', '新乡市', '开封市', '焦作市', '平顶山市', '许昌市',
  #                   '安阳市', '驻马店市', '信阳市', '鹤壁市', '周口市', '商丘市', '洛阳市', '漯河市', '濮阳市', '三门峡市', '济源市', '长沙市', '岳阳市', '衡阳市',
  #                   '株洲市', '湘潭市', '益阳市', '郴州市', '湘西土家族苗族自治州', '娄底市', '怀化市', '常德市', '张家界市', '永州市', '邵阳市', '武汉市', '黄石市',
  #                   '十堰市', '宜昌市', '襄阳市', '鄂州市', '荆门市', '孝感市', '荆州市', '黄冈市', '咸宁市', '随州市', '恩施土家族苗族自治州', '仙桃市', '潜江市',
  #                   '天门市', '神农架林区', '海口市', '万宁市', '琼海市', '三亚市', '儋州市', '东方市', '五指山市', '文昌市', '陵水黎族自治县', '澄迈县',
  #                   '乐东黎族自治县', '临高县', '定安县', '昌江黎族自治县', '屯昌县', '保亭黎族苗族自治县', '白沙黎族自治县', '琼中黎族苗族自治县', '长春市', '四平市', '辽源市',
  #                   '松原市', '吉林市', '通化市', '白山市', '白城市', '延边朝鲜族自治州',
  #                   '南京市', '苏州市', '无锡市', '连云港市', '淮安市', '扬州市', '泰州市', '盐城市', '徐州市', '常州市', '南通市', '镇江市', '宿迁市', '南昌市',
  #                   '九江市', '鹰潭市', '抚州市', '上饶市', '赣州市', '吉安市', '萍乡市', '景德镇市', '新余市', '宜春市', '沈阳市', '大连市', '盘锦市', '鞍山市',
  #                   '朝阳市', '锦州市', '铁岭市', '丹东市', '本溪市', '营口市', '抚顺市', '阜新市', '辽阳市', '葫芦岛市', '呼和浩特市', '包头市', '鄂尔多斯市',
  #                   '巴彦淖尔市', '乌海市', '阿拉善盟', '锡林郭勒盟', '赤峰市', '通辽市', '呼伦贝尔市', '乌兰察布市', '兴安盟', '银川市', '吴忠市', '固原市', '石嘴山市',
  #                   '中卫市', '西宁市', '海西蒙古族藏族自治州', '海东市', '玉树藏族自治州', '海南藏族自治州', '海北藏族自治州', '黄南藏族自治州', '果洛藏族自治州', '成都市',
  #                   '宜宾市', '绵阳市', '广元市', '遂宁市', '巴中市', '内江市', '泸州市', '南充市', '德阳市', '乐山市', '广安市', '资阳市', '自贡市', '攀枝花市',
  #                   '达州市', '雅安市', '眉山市','甘孜藏族自治州', '阿坝藏族羌族自治州', '凉山彝族自治州', '济南市', '滨州市', '青岛市', '烟台市', '临沂市', '潍坊市', '淄博市', '东营市', '聊城市', '菏泽市', '枣庄市', '德州市', '威海市', '济宁市', '泰安市', '日照市', '太原市', '大同市', '长治市', '忻州市', '晋中市', '临汾市', '运城市', '晋城市', '朔州市', '阳泉市', '吕梁市', '西安市', '铜川市', '安康市', '宝鸡市', '商洛市', '渭南市', '汉中市', '咸阳市', '榆林市', '延安市', '拉萨市', '日喀则市', '那曲市', '林芝市', '山南市', '昌都市', '阿里地区', '乌鲁木齐市', '石河子市 ', '吐鲁番市 ', '昌吉回族自治州', '哈密市', '阿克苏地区', '克拉玛依市', '博尔塔拉蒙古自治州', '阿勒泰地区', '喀什地区 ', '和田地区 ', '巴音郭楞蒙古自治州', '伊犁哈萨克自治州', '塔城地区', '克孜勒苏柯尔克孜自治州', '五家渠市', '阿拉尔市', '图木舒克市', '昆明市', '玉溪市', '楚雄彝族自治州', '大理白族自治州', '昭通市', '红河哈尼族彝族自治州', '曲靖市', '丽江市', '临沧市', '文山壮族苗族自治州', '保山市', '普洱市', '西双版纳傣族自治州', '德宏傣族景颇族自治州', '怒江傈僳族自治州', '迪庆藏族自治州', '杭州市', '丽水市', '金华市', '温州市', '台州市', '衢州市', '宁波市', '绍兴市', '嘉兴市', '湖州市', '舟山市']
  type_name_list = ['烟酒专卖店', '酒吧', '夜总会', 'KTV', '迪厅', '便利店', '便民商店/便利店','超市', '宾馆酒店', '餐饮服务']
  type_dict = {}
  dict_list = []

  for i in range(len(city_name_list)):
    type_dict = {}
    type_dict['city'] = city_name_list[i]
    for k in range(len(type_name_list)):
      type_dict[type_name_list[k]] = 0
    # type_dict['city'] = i
    dict_list.append(type_dict)
  path = "D:\BaiduNetdiskDownload\gaode-2020\\所有\\"
  fileList = os.listdir(path)
  print("共计" + str(len(fileList)) + "个文件")
  file_num = 0;
  for filename in fileList:
    file_num = file_num + 1
    sql_file_path = path + filename
    print("正在读取第" + str(file_num) + "文件" + filename)
    # if file_num == 2:
    #   break
    file = open(sql_file_path, 'r', encoding='utf-8')
    file_data = file.readlines()
    j = 0
    for row in file_data:
      if (row.isspace() or row.find("INSERT INTO") == -1):
        # print("空行或者不是正常数据行")
        continue
      start_pos = row.find("VALUES")
      end_pos = row.rfind(";")
      #得到所有的属性信息
      tmp = row[start_pos + 6:end_pos]
      # print(tmp)
      #默认是,隔开
      tmp_list = tmp.split(',')
      # 此处存在特殊情况是会将三种类别混合在一起,用;隔开
      #类别信息存在第3，或者第3，第4，第5
      type_names = tmp_list[2][2:-1]


      # 表示多种类别存在一起
      if (type_names.find(';') > 0):
        # 大类
        type_name = type_names.split(';')[0]
        # 第三个类
        small_type=type_names.split(';')[2]
      #如果不是多类别一起的话，是单独分开的
      else:
        small_type = tmp_list[4][2:-1]
        type_name = tmp_list[2][2:-1]
      city_name = tmp_list[13][2:-1]
      if city_name.isspace() or type_name.isspace():
        continue
      j = j + 1
      try:
        t = city_name_list.index(city_name)
        if (type_name in dict_list[t]):
          dict_list[t][type_name] = dict_list[t][type_name] + 1
        if (small_type in dict_list[t]):
          dict_list[t][small_type] = dict_list[t][small_type] + 1
      except ValueError:
        continue

    # print("已读取"+str(j)+"行数据")
    # print("已读完第" + str(file_num))
    file.close()
  result_excel = pd.DataFrame(dict_list)
  index = 0
  result_excel_location = "data\\2020_result_data_only_water_13.csv"
  result_excel.to_csv(result_excel_location, encoding='gbk')
    #省会城市的poi
def CalPoiNumFromSQl():
  # city_name_list = ['北京市', '上海市', '天津市', '重庆市', '长春市', '沈阳市', '哈尔滨市', '呼和浩特市', '石家庄市', '乌鲁木齐市', '兰州市', '西宁市', '西安市',
  #                   '银川市', '郑州市',
  #                   '济南市', '太原市', '合肥市', '长沙市', '武汉市', '南京市', '成都市', '贵阳市', '昆明市', '南宁市', '拉萨市', '杭州市', '南昌市', '广州市',
  #                   '福州市', '海口市', '台北市', '香港', '澳门']
  # city_name_list=['石河子市','吐鲁番市','喀什地区','和田地区','五家渠市','阿拉尔市','图木舒克市','亳州市','巢湖市','万宁市','东方市','五指山市','文昌市',
  #                 '陵水黎族自治县','澄迈县','乐东黎族自治县','临高县','定安县','昌江黎族自治县','屯昌县','保亭黎族苗族自治县','白沙黎族自治县',
  #                 '琼中黎族苗族自治县']
  city_name_list = ['广州市', '韶关市', '深圳市', '珠海市', '汕头市', '佛山市', '江门市', '湛江市', '茂名市', '肇庆市', '惠州市', '梅州市',
                    '汕尾市', '河源市', '阳江市', '清远市', '东莞市', '潮州市', '中山市', '揭阳市', '云浮市']
  type_name_dict = {};
  type_name_list = ['餐饮服务','公司企业','汽车服务','生活服务','政府机构及社会团体','购物服务','金融保险服务','医疗保健服务',
                    '科教文化服务','交通设施服务','商务住宅','地名地址信息','体育休闲服务','住宿服务','汽车维修',
                    '风景名胜','汽车销售','摩托车服务','公共设施','道路附属设施','室内设施']

  type_dict = {}
  dict_list = []
  for i in range(len(city_name_list)):
    type_dict = {}
    type_dict['city'] = city_name_list[i]
    for k in range(len(type_name_list)):
      type_dict[type_name_list[k]] = 0
    # type_dict['city'] = i
    dict_list.append(type_dict)
  path = "D:\BaiduNetdiskDownload\gaode-2020\华南\\"
  # sql_file_path='D:\BaiduNetdiskDownload\gaode-2020\SQLDumpSplitterResult\东北_1.sql'
  fileList = os.listdir(path)
  print("共计" + str(len(fileList)) + "个文件")
  file_num = 0;
  for filename in fileList:
    file_num = file_num + 1
    sql_file_path = path + filename
    print("正在读取第" + str(file_num) + "文件" + filename)
    # if file_num == 2:
    #   break
    file = open(sql_file_path, 'r', encoding='utf-8')
    file_data = file.readlines()
    j = 0
    for row in file_data:
      if (row.isspace() or row.find("INSERT INTO") == -1):
        # print("空行或者不是正常数据行")
        continue
      start_pos = row.find("VALUES")
      end_pos = row.rfind(";")
      tmp = row[start_pos + 6:end_pos]#所有的属性
      # print(tmp)
      tmp_list = tmp.split(',')#用逗号切割属性
      # 此处存在特殊情况是会将三种类别混合在一起,用;隔开
      type_names = tmp_list[2][2:-1]
      type_name = type_names
      # 表示多种类别存在一起
      if (type_names.find(';') > 0):
        type_name = type_names.split(';')[0]
      city_name = tmp_list[13][2:-1]
      # if type_name not in type_name_list:
      #   print('第'+str(j)+'行'+ ":"+type_name+":"+city_name+":")
      if city_name.isspace() or type_name.isspace():
        continue
      j = j + 1
      try:
        t = city_name_list.index(city_name)
        if type_name not in dict_list[t]:
          print('第' + str(j) + '行' + ":" + type_name + ":" + city_name + ":")
          for i in range(len(city_name_list)):
            dict_list[i][type_name] = 0
        # if file_num==11:
        #   print('第'+str(j)+'行'+ ":"+type_name+":"+city_name+":"+str(t))
        dict_list[t][type_name] = dict_list[t][type_name] + 1
      except ValueError:
        # print(admiration_sub_list[i])
        continue

    # print("已读取"+str(j)+"行数据")
    # print("已读完第" + str(file_num))
    file.close()
  result_excel = pd.DataFrame(dict_list)
  index = 0

  result_excel_location = "data\\2020_result_data_gd.csv"
  result_excel.to_csv(result_excel_location, encoding='gbk')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
  # CalAllCityPosOnlyType()
  CalPoiNumFromSQl()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
