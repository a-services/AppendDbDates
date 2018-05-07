package io.appery.dbdates;

import java.io.IOException;
import java.util.List;
import org.apache.http.impl.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.*;
import org.apache.http.util.*;
import org.apache.http.client.*;
import org.apache.http.client.utils.*;
import org.apache.http.client.entity.*;
import org.apache.http.message.*;

import groovy.json.*;

/**
 * Accessing Appery DB REST services.
 */
public class ApperyDbClient {

    CloseableHttpClient httpclient = HttpClients.createDefault();

    String dbId;
    String masterKey;
    JsonSlurper jsonSlurper;

    public ApperyDbClient(String dbId, String masterKey) {
        this.dbId = dbId; 
        this.masterKey = masterKey;
        jsonSlurper = new JsonSlurper()
    }   

    List getCollection(String collName) {
        String result;
        if (collName.equals('_users')) {
            result = makeGet('users');
        } else {
            result = makeGet('collections/' + collName);
        }
        return jsonSlurper.parseText(result)
    }

    /**
     * Performs HTTP GET.
     */
    String makeGet(String serviceUrl) throws IOException {
        HttpGet req = new HttpGet("https://api.appery.io/rest/1/db/" + serviceUrl + "?limit=1500");
        req.addHeader(new BasicHeader("Accept", "application/json"));
        req.addHeader(new BasicHeader("X-Appery-Database-Id", dbId));
        req.addHeader(new BasicHeader("X-Appery-Master-Key", masterKey));
        CloseableHttpResponse response = httpclient.execute(req);
        String result = "";
        try {
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                throw new ApperyDbException("HTTP status expected: 200, received: " + status);
            }
            result = EntityUtils.toString(response.getEntity());
            //new File('debug.json').text = result
            
        } finally {
            response.close();
        }
        sleep(500); // keep number of requests per second low
        return result;
    }

}
