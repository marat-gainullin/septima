package com.septima.http;

import com.septima.Indexer;
import com.septima.indexer.ScriptDocument;
import com.septima.script.JsDoc;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mg
 */
public class RestPoints implements Indexer.OnModule {

    private final static String GET_ANNOTATION = "@get";
    private final static String PUT_ANNOTATION = "@put";
    private final static String POST_ANNOTATION = "@post";
    private final static String DELETE_ANNOTATION = "@delete";

    private final Map<String, RpcPoint> gets = new HashMap<>();
    private final Map<String, RpcPoint> puts = new HashMap<>();
    private final Map<String, RpcPoint> posts = new HashMap<>();
    private final Map<String, RpcPoint> deletes = new HashMap<>();
    private final Map<String, Map<String, RpcPoint>> methoded = new HashMap<>(){
        {
            put("get", gets);
            put("put", puts);
            put("post", posts);
            put("delete", deletes);
        }
    };

    public RestPoints() {
        super();
    }

    public Map<String, Map<String, RpcPoint>> getMethoded() {
        return methoded;
    }

    public Map<String, RpcPoint> getGets() {
        return gets;
    }

    public Map<String, RpcPoint> getPuts() {
        return puts;
    }

    public Map<String, RpcPoint> getPosts() {
        return posts;
    }

    public Map<String, RpcPoint> getDeletes() {
        return deletes;
    }

    @Override
    public void scanned(String aModuleName, ScriptDocument.ModuleDocument aModuleDocument, File aFile) {
        Map<String, Set<JsDoc.Tag>> annotations = aModuleDocument.getPropertyAnnotations();
        if (annotations != null) {
            annotations.entrySet().stream().forEach((Map.Entry<String, Set<JsDoc.Tag>> tagsEntry) -> {
                String propName = tagsEntry.getKey();
                RpcPoint rpcPoint = new RpcPoint(aModuleName, propName);
                Set<JsDoc.Tag> tags = tagsEntry.getValue();
                tags.stream().forEach((JsDoc.Tag aTag) -> {
                    if (GET_ANNOTATION.equalsIgnoreCase(aTag.getName())) {
                        extractUri(rpcPoint, aTag, (String aUri) -> {
                            gets.put(aUri, rpcPoint);
                        });
                    }
                    if (PUT_ANNOTATION.equalsIgnoreCase(aTag.getName())) {
                        extractUri(rpcPoint, aTag, (String aUri) -> {
                            puts.put(aUri, rpcPoint);
                        });
                    }
                    if (POST_ANNOTATION.equalsIgnoreCase(aTag.getName())) {
                        extractUri(rpcPoint, aTag, (String aUri) -> {
                            posts.put(aUri, rpcPoint);
                        });
                    }
                    if (DELETE_ANNOTATION.equalsIgnoreCase(aTag.getName())) {
                        extractUri(rpcPoint, aTag, (String aUri) -> {
                            deletes.put(aUri, rpcPoint);
                        });
                    }
                });
            });
        }
    }

    private void extractUri(RpcPoint rpcPoint, JsDoc.Tag aTag, Consumer<String> withUri) {
        if (aTag.getParams() != null && !aTag.getParams().isEmpty()) {
            withUri.accept(aTag.getParams().get(0));
        } else {
            Logger.getLogger(RestPoints.class.getName()).log(Level.WARNING, "Annotation {0} in {1}.{2} missing uri parameter.", new Object[]{aTag.getName(), rpcPoint.getModuleName(), rpcPoint.getMethodName()});
        }
    }
}
