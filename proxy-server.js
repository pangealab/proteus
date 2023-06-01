const express = require('express');
const bodyParser = require('body-parser');
const Gremlin = require('gremlin');
const cors = require('cors');
const app = express();
const fs = require('fs');
const path = require('path');
const config = require('./config');

app.use(cors({
    credentials: true,
}));

// parse application/json
app.use(bodyParser.json());

// Each property has as value an array with length 1 - take first element
function mapVertexPropertiesToObj(propInObj) {
    let propOutObj = {};
    Object.keys(propInObj).forEach(k => propOutObj[k] = propInObj[k][0]);
    return propOutObj;
}

function edgesToVisualizationStructure(edges) {
    if (!!edges) {
        return edges.map(
            edge => ({
                id: typeof edge.id !== "string" ? JSON.stringify(edge.id) : edge.id,
                from: edge.from,
                to: edge.to,
                label: edge.label,
                properties: edge.properties,
            })
        );
    } else {
        return [];
    }
}

function nodesToVisualizationStructure(nodeList) {
    return nodeList.map(
        node => ({
            id: node.id,
            label: node.label,
            properties: mapVertexPropertiesToObj(node.properties),
            edges: edgesToVisualizationStructure(node.edges)
        })
    );
}

function makeSelfQuery(query) {
    const theQuery = `${query}.
        as('node').
        project('id', 'label', 'properties').
        by(__.id()).
        by(__.label()).
        by(__.valueMap())
    `;
    return theQuery;
}

function makeInQuery(query, nodeLimit) {
    // original query: `${query}${nodeLimitQuery}.dedup().as('node').project('id', 'label', 'properties', 'edges').by(__.id()).by(__.label()).by(__.valueMap().by(__.unfold())).by(__.outE().project('id', 'from', 'to', 'label', 'properties').by(__.id()).by(__.select('node').id()).by(__.inV().id()).by(__.label()).by(__.valueMap().by(__.unfold())).fold())`;
    const nodeLimitQuery = !isNaN(nodeLimit) && Number(nodeLimit) > 0 ? `.limit(${nodeLimit})` : '';
    const theQuery = `${query}${nodeLimitQuery}.
        dedup().
        as('node').
        project('id', 'label', 'properties', 'edges').
        by(__.id()).
        by(__.label()).
        by(__.valueMap()).
        by(__.outE().as('outEdge').
            project('id', 'from', 'to', 'label', 'properties').
            by(__.id()).
            by(select('node').id()).
            by(__.inV().id()).
            by(__.label()).
            by(__.valueMap()).
            fold()
        )
    `; 
    // coalesce(select('outEdge').inV().count().is(gt(0)).select('outEdge').inV().id(), constant("NO_TO_VERTEX"))
    return theQuery;
}

function makeOutQuery(query, nodeLimit) {
    // original query: `${query}${nodeLimitQuery}.dedup().as('node').project('id', 'label', 'properties', 'edges').by(__.id()).by(__.label()).by(__.valueMap().by(__.unfold())).by(__.outE().project('id', 'from', 'to', 'label', 'properties').by(__.id()).by(__.select('node').id()).by(__.inV().id()).by(__.label()).by(__.valueMap().by(__.unfold())).fold())`;
    const nodeLimitQuery = !isNaN(nodeLimit) && Number(nodeLimit) > 0 ? `.limit(${nodeLimit})` : '';
    const theQuery = `${query}${nodeLimitQuery}.
        dedup().
        as('node').
        project('id', 'label', 'properties', 'edges').
        by(__.id()).
        by(__.label()).
        by(__.valueMap()).
        by(__.inE().
            project('id', 'from', 'to', 'label', 'properties').
            by(__.id()).
            by(__.outV().id()).
            by(select('node').id()).
            by(__.label()).
            by(__.valueMap()).
            fold()
        )
    `;
    return theQuery;
}

async function executeQuery(query) {
    const authenticator = new Gremlin.driver.auth.PlainTextSaslAuthenticator(`/dbs/${config.database}/colls/${config.collection}`, config.primaryKey)

    const client = new Gremlin.driver.Client(
        config.endpoint, 
        { 
            authenticator,
            traversalsource : "g",
            rejectUnauthorized : true,
            mimeType : "application/vnd.gremlin-v2.0+json"
        }
    );

    console.log(query);
    try {
        const result = await client.submit(query, {})
        console.log(JSON.stringify(result, null, 2));
        return result;
    }
    catch(err) {
        console.error(err);
        return null;   
    }
}

app.post('/query', async (req, res, next) => {
    const nodeLimit = req.body.nodeLimit;
    let query = "" + req.body.query;
    let visualizationNodesAndEdges = [];

    // Support for sample files to show possible  
    if (query.startsWith("sample:")) {
        try {
            const sample = query.split(":")[1];
            visualizationNodesAndEdges = JSON.parse(fs.readFileSync(path.join(__dirname, "samples", `${sample}.json`), 'utf8'));
        }
        catch(err) {
            console.error(err);
        }
    } else {
        let theQuery;
        if(query.endsWith(".out()")) {
            theQuery = makeOutQuery(query, nodeLimit);
        } else if (query.endsWith(".in()")) {
            theQuery = makeInQuery(query, nodeLimit);
        } else {
            theQuery = makeSelfQuery(query);
        }

        const result = await executeQuery(theQuery);
        if (result !== null) {
            visualizationNodesAndEdges = nodesToVisualizationStructure(result._items);
        }
    }

    const visualizationNodesAndEdgesPrettyfiedJSon = JSON.stringify(visualizationNodesAndEdges, null, 2);
    console.log(visualizationNodesAndEdgesPrettyfiedJSon);
    res.send(visualizationNodesAndEdgesPrettyfiedJSon);
});

app.get('/edgecount/:nodeId', async (req, res, next) => {
    const nodeId = req.params.nodeId;
    let query = `g.V("${nodeId}").project("inEdgesCount", "outEdgesCount").by(__.inE().count()).by(__.outE().count())`;
    const result = await executeQuery(query); // result._items in format: [ { "inEdgesCount": 2, "outEdgesCount": 0 } ]
    let countInfo;
    if (result === null || result._items.length === 0) {
        countInfo = { 'inEdgesCount': -1, 'outEdgesCount': -1}; // error - node does not exist?
    } else {
        countInfo = result._items[0];
    }
    res.send(JSON.stringify(countInfo, null, 2));
});

app.listen(config.port, () => console.log(`Simple Gremlin proxy-server listening on port ${config.port}!`));