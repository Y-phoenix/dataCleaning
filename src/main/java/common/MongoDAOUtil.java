package common;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class MongoDAOUtil {

    private static MongoDAOUtil instance = new MongoDAOUtil();

    //private static MongoDAOUtil readInstance = new MongoDAOUtil();

    public MongoClient mongoClient;
    public DB db;


//	private static boolean enabled = true;

    public MongoDAOUtil() {

    }

    public static MongoDAOUtil getInstance() {
        return instance;
    }

    public static void setupMongodb(String mongoHost,
                                    int mongoPort,
                                    String mongoDatabase,
                                    Integer connectTimeOut,
                                    Integer connectionsPerHost,
                                    String username,
                                    String password
    ) throws UnknownHostException {
        MongoClientOptions options = MongoClientOptions.builder()
                .connectTimeout(connectTimeOut == null ? 3000 : connectTimeOut)
                .connectionsPerHost(connectionsPerHost == null ? 200 : connectionsPerHost)  //sets the connection timeout to 3 seconds
                //.autoConnectRetry( true )
                .build();

        //MongoCredential credential2 = MongoCredential.createPlainCredential(username, "admin", password.toCharArray());
        MongoCredential credential = MongoCredential.createCredential(username, "admin", password.toCharArray());
        MongoClient mongoClient =
                new MongoClient(
                        new ServerAddress(mongoHost, mongoPort),
                        Arrays.asList(credential),
                        options);

        if (instance.mongoClient != null) {
            instance.mongoClient.close();
        }
        instance.mongoClient = mongoClient;
        DB db = mongoClient.getDB(mongoDatabase);
//        Mongo mongo = new Mongo("localhost", 27017);
//        DB db = mongo.getDB(mongoDatabase);
        instance.db = db;

    }


    public DBCollection getCollection (String name){
        return db.getCollection(name);
    }


}
