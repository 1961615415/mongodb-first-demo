package com.dcx.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
/***
 * 1.下载安装包 https://www.mongodb.com/download-center#community
 * 2.配置mongod --dbpath "D:\softkf\mongodb\db" --logpath "D:\softkf\mongodb\log\mongodb.log" --logappend (这样就可以从服务里面来启动)
 * 3.启动数据库 进入bin目录  执行mongo.exe命令
 * 4.db 来查看所有非空的数据库
 * @author duchunxia
 *
 */
public class MongoDbDemo {
	MongoClient  mongoClient;
	MongoDatabase mongoDatabase;
	long startDate;
/***
 * 初始化链接到MongoDB
 */
@Before
public void init(){
	startDate=System.currentTimeMillis();
	mongoClient=new MongoClient("127.0.0.1", 27017 );
	mongoDatabase=mongoClient.getDatabase("dcx");
}
/***
 * 关闭数据库操作
 */
@After
public void close(){
	mongoClient.close();
	System.out.println((System.currentTimeMillis()-startDate)/60);
}
/***---------------集合操作--------------**/
/***
 * 不用单一来创建，创建文档时将自动创建集合
 */
@Test
public void createCollection(){
	mongoDatabase.createCollection("c1");
}
/***
 * 删除集合
 */
@Test
public void dropCollection(){
	mongoDatabase.drop();
}
/***
 * 获取集合
 */
@Test
public void findCollection(){
	ListCollectionsIterable listCollectionsIterable=mongoDatabase.listCollections();
	for (Object object : listCollectionsIterable) {
		System.out.println(object);
	}
}
/*******-------------文档操作------------------****/
/***
 * 创建一个文档
 */
@Test
public void createOneDocument(){
	MongoCollection mongoCollection=mongoDatabase.getCollection("c2");
	Document document=new Document("name", "张三");
	document.put("sex", "男");
	mongoCollection.insertOne(document);
	Document document2=new Document("bookname", "spring");
	document2.put("bookdesc", "关于spring的基础");
	document2.put("price",980.1);
	mongoCollection.insertOne(document2);
}
/***
 * 创建多个文档
 * 
 */
@Test
public void createMoreDocument(){
	MongoCollection mongoCollection=mongoDatabase.getCollection("c2");
	List<Document> documents=new ArrayList<Document>();
	for (int i = 0; i < 100; i++) {
		Document document=new Document("bookname", "spring"+i);
		document.put("bookdesc", "关于spring的基础");
		document.put("price",i);
		document.append("edit", "非著名作者");
		documents.add(document);
	}
	mongoCollection.insertMany(documents);
}

/***
 * 获取所有的文档
 */
@Test
public void getAllDocument(){
	MongoCollection mongoCollection=mongoDatabase.getCollection("c2");
	FindIterable findIterable=mongoCollection.find();
	for (Object object : findIterable) {
		System.out.println(object);
		//Document document=(Document)object;
		//System.out.println(document.get("name"));
	}
}
/***
 * 分页获取数据
 */
@Test
public void getDocumentByPage(){
	MongoCollection mongoCollection=mongoDatabase.getCollection("c2");
	//每页显示10条 从0条开始显示
	FindIterable findIterable=mongoCollection.find().skip(10).limit(10);
	for (Object object : findIterable) {
		System.out.println(object);
	}
}
/***
 * 条件查询
 */
@Test
public void getDocumentBytj(){
	MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("c2");
	//where name="张三"============db.c2.find({"name":"张三"})
	FindIterable<Document> findIterable=mongoCollection.find(Filters.eq("name","张三"));
	for (Document document : findIterable) {
		System.out.println(document);
	}
	//where price>=98 and bookdesc="关于spring的基础"============== db.c2.find({"price":{$gte:98}},{"bookdesc":"关于spring的基础"})
	List<Bson> filters=new ArrayList<Bson>();
	Bson bson=Filters.gte("price", 98);
	Bson bson2=Filters.eq("bookdesc", "关于spring的基础");
	filters.add(bson);
	filters.add(bson2);
	FindIterable<Document> findIterable2=mongoCollection.find(Filters.and(filters));
	for (Document document : findIterable2) {
		System.out.println(document);
	}
	
	//where price=98 or name="张三"======================db.c2.find({$or:[{"price":{$eq:98}},{name:"张三"}]})
	List<Bson> filters2=new ArrayList<Bson>();
	filters2.add(Filters.eq("price", 98));
	filters2.add(Filters.eq("name","张三"));
	FindIterable<Document> findIterable3=mongoCollection.find(Filters.or(filters2));
	for (Document document : findIterable3) {
		System.out.println(document);
	}
	//where name like '%王%'   ======= db.c2.find({"name":/王/}) 程序?怎么表示
	
}
/***
 * 文档的排序
 */
@Test
public void getDocumetsort(){
	MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("c2");
	Document sort=new Document();
	//order by  price desc ============= db.c2.find().sort({"price":-1})
	sort.put("price", -1);
	FindIterable<Document> findIterable=mongoCollection.find().sort(sort);
	System.out.println("按照价格的降序");
	for (Document document : findIterable) {
		System.out.println(document);
	}
	//order by  price asc==============db.c2.find().sort({"price":1})
	sort.put("price", 1); 
	FindIterable<Document> findIterable2=mongoCollection.find().sort(sort);
	System.out.println("按照价格的升序");
	for (Document document : findIterable2) {
		System.out.println(document);
	}
}
/***
 * skip(), limit(), sort()三个放在一起执行的时候，执行的顺序是先 sort(), 然后是 skip()，最后是显示的 limit()。
 */
@Test
public  void getDocumentSkipAndSort(){
	MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("c2");
	Document sort=new Document();
	//order by  price desc==============db.c2.find().limit(100).sort({"price":1}).skip(0)
	sort.put("price", -1); 
	FindIterable<Document> findIterable=mongoCollection.find().sort(sort).skip(0).limit(10);
	System.out.println("按照价格的降序");
	for (Document document : findIterable) {
		System.out.println(document);
	}
}
/***
 * 更新文档
 * updateOne 是只更新其中的一条数据
 * updateMany 更新所有符合条件的数据
 */
@Test
public void updateDocu(){
	//============= db.c2.update({"name":"王五"},{$set:{"name":"张三"}}))
	MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("c2");
	Document update=new Document();
	Document bson=new Document("name","王五");
	update.put("$set", bson);
	UpdateResult updateResult=mongoCollection.updateOne(Filters.eq("name", "张三"), update);
	System.out.println("更新的条数："+updateResult.getModifiedCount());
	getAllDocument();
	UpdateResult updateResult2=mongoCollection.updateMany(Filters.eq("name", "李四"), update);
	System.out.println("更新的条数："+updateResult2.getModifiedCount());
	getAllDocument();
}
/***
 * 删掉文档
 * deleteOne 只删掉符合条件的一条
 * deleteMany 删掉所有符合条件的数据
 */
@Test
public void deleteDocu(){
	//==============db.c2.remove({"name":"张三"})
	MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("c2");
	/*DeleteResult  deleteResult=mongoCollection.deleteOne(Filters.not(Filters.eq("name","王五")));
	System.out.println("删掉的条数："+deleteResult.getDeletedCount());*/
	DeleteResult  deleteResultMany=mongoCollection.deleteMany(Filters.not(Filters.eq("name","王五")));
	System.out.println("删掉的条数："+deleteResultMany.getDeletedCount());
	getAllDocument();
}
}
