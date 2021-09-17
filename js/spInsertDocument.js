function insertDocument(document) {
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();
    var response = getContext().getResponse();

    if (!document) throw new Error("The document is undefined or null.");

    tryCreate(document, callback);

    function tryCreate(document, callback) {
        var options = { disableAutomaticIdGeneration: true };
        var isAccepted = collection.createDocument(collectionLink, document, options, callback);
        if (!isAccepted) { response.setBody(0); }
        else {response.setBody("Great Success"); }
    }

    function callback(err, document, options) {
        if (err) { throw err; }
        else { tryCreate(document, callback); }
    }
}