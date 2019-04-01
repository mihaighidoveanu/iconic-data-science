// Databricks notebook source
// MAGIC %md
// MAGIC # This does not work
// MAGIC This is used to vizualize networks in a JavaScript manner

// COMMAND ----------

// MAGIC %md
// MAGIC ### Initial Setup
// MAGIC We access the Azure Data Lake Storage with Service to Service authentication. Spark uses an Active Directory app to read and write from the ADLS . Next we will set up the credentials for the app and the active directory of the owner account
// MAGIC We also initialize some global variables with the path to the ADLS. The variable will be given as an argument anywhere a function needs to read from the MAG data.

// COMMAND ----------

// Credentials
val client_id = sys.env("AZURE_CLIENT_KEY")
val client_secret = sys.env("AZURE_SECRET_KEY")
val tenant_id = sys.env("AZURE_TENANT_ID")

// Spark-to-ADLS configurations for Dataframes and Datasets
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", client_id)
spark.conf.set("dfs.adls.oauth2.credential", client_secret)
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token")

// Spark-to-ADLS configurations for RDD
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", client_id)
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", client_secret)
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token")

// path variables
val adls = "magdls"     // adls name
val mag_dir = "graph/2018-06-21"  // directory with the mag data
val smag_dir = "graph/samples" //directory with the sample mag data
val mag = "adl://" + adls + ".azuredatalakestore.net/" + mag_dir
val smag = "adl://" + adls + ".azuredatalakestore.net/" + smag_dir
val ssmag = "adl://" + adls + ".azuredatalakestore.net/graph/Samples"
val gmag = "adl://" + adls + ".azuredatalakestore.net/graph/gexf"

// DBFS-to-ADLS mount point
val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> client_id,
  "dfs.adls.oauth2.credential" -> client_secret,
  "dfs.adls.oauth2.refresh.url" -> ("https://login.microsoftonline.com/" + tenant_id + "/oauth2/token"))

val gexf_mount = "/mnt/gexf"

// clear up
dbutils.fs.unmount(gexf_mount)
// mount the data lake 
dbutils.fs.mount(
  source = "adl://" + adls + ".azuredatalakestore.net/" + gmag,
  mountPoint = "/mnt/gexf",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Define needed Case Classes

// COMMAND ----------

import java.sql.Date
case class Author(id: Long, rank: Long, name: String, dname: String, 
                  affiliation: Long, papers: Long, citations:Long, createdAt:Date)
case class Link(src:Long, dst: Long, papersTogether: Long)
case class DLink(src: Long, dst: Long, citations: collection.mutable.Map[Int,collection.mutable.Map[Long,Long]])

// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC ## Load the networks from ADLS

// COMMAND ----------

import org.apache.spark.sql.Dataset
def toGexf(net : (Dataset[Author],Dataset[Link]) ) : String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"https://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "\t<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "\t\t<nodes>\n" +
    net._1.map(v => 
    "\t\t\t<node id=\"" + v.id + "\" label=\"" + v.dname + "\" />\n").collect.mkString +
    "\t\t</nodes>\n" +
    "\t\t<edges>\n" +
    net._2.map(e => 
    "\t\t\t<edge source=\"" + e.src + "\" target=\"" + e.dst + "\" label=\"" + e.papersTogether + "\" />\n").collect.mkString +
    "\t\t</edges>\n" +
    "\t</graph>\n" +
    "</gexf>"
}

// COMMAND ----------

for(index <- 1 to 3) {
  val dfVertex = spark.read.parquet(ssmag + "/Vertex2_" + index).as[Author]
  val dfLinks = spark.read.parquet(ssmag + "/Links2_" + index).as[Link]
  val net = (dfVertex,dfLinks)
  val graph = toGexf(net)
  sc.parallelize(Seq(graph)).coalesce(1).saveAsTextFile(gmag + "/Graph2_" + index)
}

// COMMAND ----------



// COMMAND ----------

val style = """
<style>
#modal {
    position:fixed;
    left:150px;
    top:20px;
    z-index:1;
    background: white;
    border: 1px black solid;
    box-shadow: 10px 10px 5px #888888;
    display: none;
}
  
#content {
    max-height: 400px;
    overflow: auto;
}
  
#modalClose {
    position: absolute;
    top: -0px;
    right: -0px;
    z-index: 1;
}
tr {
  border: 1px gray solid;
}

td {
  font-size: 10px;
}
td.data {
    font-weight: 900;
}
  
.tick line {
  shape-rendering: crispEdges;
  stroke: #000;
}

line.minor  {
  stroke: #777;
  stroke-dasharray: 2,2;
}

path.domain {
  fill: none;
  stroke: black;
}

.inactive, .tentative {
  stroke: darkgray;
  stroke-width: 4px;
  stroke-dasharray: 5 5;
}

.tentative {
  opacity: .5;
}

.active {
  stroke: black;
  stroke-width: 4px;
  stroke-dasharray: 0;
}

circle {
  fill: red;
}

rect {
  fill: darkgray;
}

#controls {
  position: fixed;
  bottom: 50px;
  left: 20px;
}
#brushDiv {
  position: fixed;
  bottom: 100px;
  left: 20px;
  right: 20px;
  height:50px;
  background: white;
  opacity: .75;
}

.brush .extent {
  fill-opacity: .90;
  shape-rendering: crispEdges;
}

svg {
    width: 100%;
    height:100%;
}
</style>
"""

// COMMAND ----------

val script = s""" 
<script src="https://d3js.org/d3.v3.min.js"></script>
<script src="https://d3js.org/colorbrewer.v1.min.js"></script>
<script>
  nodeFocus = false;
  currentBrush =[0,0];
  docHash = {};
  allLinks = [];
  currentScale = 0;
  
  function loadGraph() {
    sourceGEXF = `${graph}`;
    newGEXF = GexfParser.fetch(sourceGEXF);
    gD3 = gexfD3().graph(newGEXF).size([1000,1000]).nodeScale([5,20]);

    force = d3.layout.force()
      .charge(-500)
      .linkDistance(200)
      .size([1000, 1000])
      .gravity(.1)
      .on("tick", redrawGraph)

    zoom = d3.behavior.zoom()
    .scaleExtent([.1, 10])
    .on("zoom", zoomed);

    allLinks = gD3.links();
    
    d3.select("svg").call(zoom);
    createControls();
    zoomed();
    draw();

  }
      
      function highlightNeighbors(d,i) {
        var nodeNeighbors = findNeighbors(d,i);
        d3.selectAll("g.node").each(function(p) {
          var isNeighbor = nodeNeighbors.nodes.indexOf(p);
          d3.select(this).select("circle")
          .style("opacity", isNeighbor > -1 ? 1 : .25)
          .style("stroke-width", isNeighbor > -1 ? 3 : 1)
          .style("stroke", isNeighbor > -1 ? "blue" : "white")
        })
        
        d3.selectAll("line.link")
        .style("stroke-width", function (d) {return nodeNeighbors.links.indexOf(d) > -1 ? 2 : 1})
        .style("opacity", function (d) {return nodeNeighbors.links.indexOf(d) > -1 ? 1 : .25})
      }
      
      function findNeighbors(d,i) {
        neighborArray = [d];
        var linkArray = [];
        var linksArray = d3.selectAll("line.link").filter(function(p) {return p.source == d || p.target == d}).each(function(p) {
          neighborArray.indexOf(p.source) == -1 ? neighborArray.push(p.source) : null;
          neighborArray.indexOf(p.target) == -1 ? neighborArray.push(p.target) : null;
          linkArray.push(p);
        })
//        neighborArray = d3.set(neighborArray).keys();
        return {nodes: neighborArray, links: linkArray};
      }

    function zoomed() {
    force.stop();
    var canvWidth = parseInt(d3.select("#vizcontainer").style("width"));
    var canvHeight = parseInt(d3.select("#vizcontainer").style("height"));
      if (currentScale != zoom.scale()) {
        currentScale = zoom.scale();
        var halfCanvas = canvHeight / 2;
        var zoomLevel = halfCanvas * currentScale;
        gD3.xScale().range([halfCanvas - zoomLevel, halfCanvas + zoomLevel]);
        gD3.yScale().range([halfCanvas + zoomLevel, halfCanvas - zoomLevel]);
        redrawGraph();
      }
      var canvasTranslate = zoom.translate();
      d3.select("#graphG").attr("transform", "translate("+canvasTranslate[0]+","+canvasTranslate[1]+")")
    }
    
    function createControls() {
      
      d3.select("#controls").append("button").attr("class", "origButton").html("Force On").on("click", function() {
      force.start();})
      d3.select("#controls").append("button").attr("class", "origButton").html("Force Off").on("click", function() {
      force.stop();})
      d3.select("#controls").append("button").attr("class", "origButton").html("Reset Layout").on("click", function() {
      force.stop();
      gD3.nodes().forEach(function (el) {el.x = el.originalX;el.px = el.originalX;el.y = el.originalY;el.py = el.originalY;});
      currentBrush = [0,0];
      draw();
      redrawGraph();
      })

      d3.select("#controls").append("button").attr("class", "origButton").html("Reset Colors").on("click", function() {
      var sizeScale = gD3.nodeScale();
      d3.selectAll("circle")
      .attr("r", function (d) {return sizeScale(d.size)})
      .style("fill", function(d) {return d.rgbColor})
      .style("opacity", 1);
      d3.selectAll("line.link").style("stroke", "black");
      })
      
      d3.select("#controls").selectAll("button.nodeButtons").data(gD3.nodeAttributes())
      .enter()
      .append("button")
      .attr("class", "nodeButtons")
      .on("click", nodeButtonClick)
      .html(function(d) {return d});

      d3.select("#controls").selectAll("button.linkButtons").data(gD3.linkAttributes())
      .enter()
      .append("button")
      .attr("class", "linkButtons")
      .on("click", linkButtonClick)
      .html(function(d) {return d});

    }
    
    function nodeButtonClick(d,i) {
      var nodeAttExtent = d3.extent(gD3.nodes(), function(p) {return parseFloat(p.properties[d])});
      var colorScale = d3.scale.quantize().domain(nodeAttExtent).range(colorbrewer.YlGnBu[6]);
      d3.selectAll("circle").style("fill", function(p) {return colorScale(p.properties[d])}).style("opacity", 1)
    }
    function linkButtonClick(d,i) {
      var linkAttExtent = d3.extent(gD3.links(), function(p) {return parseFloat(p.properties[d])});
      var colorScale = d3.scale.quantize().domain(linkAttExtent).range(colorbrewer.YlGnBu[6]);
      d3.selectAll("line").style("stroke", function(p) {return colorScale(p.properties[d])}).style("opacity", 1)      
    }
    
    function redrawGraph() {
      var xScale = gD3.xScale();
      var yScale = gD3.yScale();

      d3.selectAll("line.link")
      .attr("x1", function (d) {return xScale(d.source.x)})
      .attr("x2", function (d) {return xScale(d.target.x)})
      .attr("y1", function (d) {return yScale(d.source.y)})
      .attr("y2", function (d) {return yScale(d.target.y)});

      d3.selectAll("g.node")
      .attr("transform", function(d) {return "translate(" + xScale(d.x) + "," + yScale(d.y) + ")"});
    }

    function draw() {
      var xScale = gD3.xScale();
      var yScale = gD3.yScale();
      var sizeScale = gD3.nodeScale();

      var forceRunning = false;
      if (force.alpha() > 0) {
        force.stop();
        forceRunning = true;
      }

      d3.select("#graphG").selectAll("line.link")
      .data(gD3.links(), function (d) {return d.id})
      .enter()
      .insert("line", "g.node")
      .attr("class","link")
      .attr("x1", function (d) {return xScale(d.source.x)})
      .attr("x2", function (d) {return xScale(d.target.x)})
      .attr("y1", function (d) {return yScale(d.source.y)})
      .attr("y2", function (d) {return yScale(d.target.y)})
      .style("stroke", "black")
      .style("stroke-width", "1px")
      .style("opacity", .25)

      d3.select("#graphG").selectAll("g.node").data(gD3.nodes(), function (d) {return d.id})
      .enter()
      .append("g")
      .attr("class", "node")
      .attr("transform", function(d) {return "translate(" + xScale(d.x) + "," + yScale(d.y) + ")"})
      .on("mouseover", nodeOver)
      .on("mouseout", nodeOut)
      .on("click", nodeClick)
      .append("circle")
      .attr("r", function(d) {return sizeScale(d.size)})
      .style("fill", function(d) {return d.rgbColor})
      .style("stroke", "black")
      .style("stroke-width", "1px")
      .style("stroke-opacity", 1);

      force
      .nodes(gD3.nodes())
      .links(gD3.links());
      
        function nodeOver(d,i,e) {
        var el = this;
        if (!d3.event.fromElement) {
          el = e;
        }
        if (nodeFocus) {
          return;
        }
        //Only do the element stuff if this came from mouseover
        el.parentNode.appendChild(el);
        d3.select(el).append("text").attr("class", "hoverLabel").attr("stroke", "white").attr("stroke-width", "5px")
        .style("opacity", .9)
        .style("pointer-events", "none")
        .text(d.label);
        
        d3.select(el).append("text").attr("class", "hoverLabel")
        .style("pointer-events", "none")
        .text(d.label);
        highlightNeighbors(d,i);
      }
      
      function nodeClick(d,i) {
        nodeFocus = false;
        nodeOut();
        nodeOver(d,i,this);
        nodeFocus = true;
        var newContent = "<p>" + d.label + "</p>";
        newContent += "<p>Attributes: </p><p><ul>";
        for (x in gD3.nodeAttributes()) {
          newContent += "<li>" + gD3.nodeAttributes()[x] + ": " + d.properties[gD3.nodeAttributes()[x]]+ "</li>";          
        }
        newContent += "</ul></p><p>Connections:</p><ul>";
        var neighbors = findNeighbors(d,i);
        for (x in neighbors.nodes) {
          if (neighbors.nodes[x] != d) {
            newContent += "<li>" + neighbors.nodes[x].label + "</li>";
          }
        }
        newContent += "</ul></p>";
        
        d3.select("#modal").style("display", "block").select("#content").html(newContent);
      }

    }

      function nodeOut() {
        if (nodeFocus) {
          return;
        }

        d3.selectAll(".hoverLabel").remove();
        d3.selectAll("circle").style("opacity", 1).style("stroke", "black").style("stroke-width", "1px");
        d3.selectAll("line").style("opacity", .25);
      }


</script>
"""

// COMMAND ----------

val d3GEFX = """
<script>
gexfD3 = 
  function () {

      var nodes = [];
      var links = [];
      var linksFile = "";
      var fileName = "";
      var xExtent = [];
      var yExtent = [];
      var nodeScale = [1,10];
      var layoutSize = [500,500];
      var sizeExtent = [];
      var dAtt = "";
      var dynamicExtent = [];
      var sizeScale, xScale, yScale, dynamicScale;
      var gexfD3Brush = d3.svg.brush();
      var linkAttributes = [];
      var nodeAttributes = [];
      var nodeHash = {};

      this.graph = function(gexfParsed) {
        if (!arguments.length) return true;

      var gNodes = gexfParsed.nodes;
      var gLinks = gexfParsed.edges;

      nodes = [];
      links = [];
      nodeHash = {};
      //Create JSON nodes array      
      var x = 0;
      gNodes.forEach(function(gNode) {
        var newNode = {id: x, properties: {}};
        newNode.label = gNode.label || gNode.id;
        newNode.rgbColor = gNode.viz.color || "rgb(122,122,122)";
        newNode.x = gNode.viz.position.x;
        newNode.y = gNode.viz.position.y;
        newNode.z = gNode.viz.position.z;
        newNode.originalX = newNode.x;
        newNode.originalY = newNode.y;
        newNode.size = gNode.viz.size;
        nodeHash[gNode.id] = newNode;
        for (y in gNode.attributes) {
          if (!(typeof(gNode.attributes[y]) === "undefined") && !(gNode.attributes[y].toString() == "NaN" )) {
            newNode.properties[y] = gNode.attributes[y];
          }
        }
        nodes.push(newNode);
        x++;
      })
      //get node attributes based on attributes in the first node
      //this won't work for assymetrical node attributes
      nodeAttributes = d3.keys(nodes[0].properties);

      //Create JSON links array
      var x = 0;
      while (x < gLinks.length) {
        var newLink = {id: x, properties: {}};
        newLink.source = nodeHash[gLinks[x].source];
        newLink.target = nodeHash[gLinks[x].target];
        //process attributes
        for (y in gLinks[x].attributes) {
        newLink.properties[y] = gLinks[x].attributes[y];
        y++;
        }
        links.push(newLink)
        x++;
      }
      linkAttributes = d3.keys(links[0].properties);

        sizeExtent = d3.extent(nodes, function(d) {return parseFloat(d.size)})
        sizeScale = d3.scale.linear().domain(sizeExtent).range(nodeScale);
        return this;
      }
      this.nodes = function(incNodes) {
        if (!arguments.length) return nodes;
        nodes = incNodes;
        return this;
      }
      this.links = function(incLinks) {
        if (!arguments.length) return links;
        links = incLinks
        return this;
      }
      
      this.linkAttributes = function(incAtts) {
        if (!arguments.length) return linkAttributes;
        linkAttributes = incAtts;
        return this;
      }

      this.nodeAttributes = function(incAtts) {
        if (!arguments.length) return nodeAttributes;
        nodeAttributes = incAtts;
        return this;
      }
      
      this.nodeScale = function(incScale) {
        if (!arguments.length) return sizeScale;
        nodeScale = incScale;
        sizeScale = d3.scale.linear().domain(sizeExtent).range(nodeScale);
        return this;
      }



this.overwriteLinks = function(incLinks) {
      if (!arguments.length) return nodes;
      data = incLinks;
      //OVERWRITE links for parallel links
      links = [];
        for (x in data) {
        var newLink = {id: x, properties: {}};
        newLink.source = nodeHash[data[x].source];
        newLink.target = nodeHash[data[x].target];
        newLink.id = x;
        newLink.properties.type = "base";
        newLink.properties.year = data[x].year;
        //process attributes
        if (newLink.source && newLink.target) {
          links.push(newLink);
        }
        x++;          
        }
        linkAttributes = d3.keys(links[0].properties);
      
      return this;
      }

      this.size = function(incSize) {
      if (!arguments.length) return layoutSize;

      //Measure
      layoutSize = incSize;
      xExtent = d3.extent(nodes, function(d) {return parseFloat(d.x)})
      yExtent = d3.extent(nodes, function(d) {return parseFloat(d.y)})
      xScale = d3.scale.linear().domain(xExtent).range([0,layoutSize[0]]);
      yScale = d3.scale.linear().domain(yExtent).range([layoutSize[1],0]);
      return this;
      }

      this.dynamicAttribute = function(incAtt) {
      if (!arguments.length) return dAtt;
        dAtt = incAtt;
        var nDE = [Infinity, -Infinity];
        var lDE = [Infinity, -Infinity];        
        if (nodeAttributes.indexOf(dAtt) > -1) {
          //currently filters out 0 entries
//          nDE = d3.extent(nodes, function(d) {return parseInt(d.properties[dAtt])})
          nDE = d3.extent(nodes.filter(function(p) {return p.properties[dAtt] != 0}), function(d) {return parseInt(d.properties[dAtt])})
        }
        if (linkAttributes.indexOf(dAtt) > -1) {
//          lDE = d3.extent(links, function(d) {return parseInt(d.properties[dAtt])})
          lDE = d3.extent(links.filter(function(p) {return p.properties[dAtt] != 0}), function(d) {return parseInt(d.properties[dAtt])})
        }
        dynamicExtent = [Math.min(nDE[0],lDE[0]), Math.max(nDE[1],lDE[1])]
        dynamicScale = d3.scale.linear().domain(dynamicExtent).range([0,layoutSize[0]]);
      return this;
      }
      
      this.dynamicBrush = function(incSelection) {
      if (!arguments.length) return gexfD3Brush;
    gexfD3Brush
    .x(dynamicScale)
    .extent(dynamicExtent)
    var brushAxis = d3.svg.axis().scale(dynamicScale).orient("bottom").tickSize(-40).ticks(20);

      incSelection.append("g").attr("id", "bgAxis").append("g").attr("transform", "translate(50,35)").call(brushAxis)
      incSelection.append("g").attr("id", "fgBrush").attr("transform", "translate(50,0)")
      .call(gexfD3Brush)
      .selectAll("rect").attr("height", 35);
      return this;
      }
      
      this.xScale = function(newScale) {
      if (!arguments.length) return xScale;
      xScale = newScale;
      return this;        
      }
      
      this.yScale = function(newScale) {
      if (!arguments.length) return yScale;
      yScale = newScale;
      return this;        
      }

      return this;
    }
    </script>
    """

// COMMAND ----------

val parser = """
<script>
;(function(undefined) {
  'use strict';

  /**
   * GEXF Parser
   * ============
   *
   * Author: PLIQUE Guillaume (Yomguithereal)
   * URL: https://github.com/Yomguithereal/gexf-parser
   * Version: 1.0
   */

  /**
   * Helper Namespace
   * -----------------
   *
   * A useful batch of function dealing with DOM operations and types.
   */
  var _helpers = {
    nodeListToArray: function(nodeList) {

      // Return array
      var children = [];

      // Iterating
      for (var i = 0, len = nodeList.length; i < len; ++i) {
        if (nodeList[i].nodeName !== '#text')
          children.push(nodeList[i]);
      }

      return children;
    },
    nodeListEach: function(nodeList, func) {

      // Iterating
      for (var i = 0, len = nodeList.length; i < len; ++i) {
        if (nodeList[i].nodeName !== '#text')
          func(nodeList[i]);
      }
    },
    nodeListToHash: function(nodeList, filter) {

      // Return object
      var children = {};

      // Iterating
      for (var i = 0; i < nodeList.length; i++) {
        if (nodeList[i].nodeName !== '#text') {
          var prop = filter(nodeList[i]);
          children[prop.key] = prop.value;
        }
      }

      return children;
    },
    namedNodeMapToObject: function(nodeMap) {

        // Return object
      var attributes = {};

      // Iterating
      for (var i = 0; i < nodeMap.length; i++) {
        attributes[nodeMap[i].name] = nodeMap[i].value;
      }

      return attributes;
    },
    getFirstElementByTagNS: function(node, ns, tag) {
      var el = node.getElementsByTagName(ns + ':' + tag)[0];

      if (!el)
        el = node.getElementsByTagNameNS(ns, tag)[0];

      if (!el)
        el = node.getElementsByTagName(tag)[0];

      return el;
    },
    getAttributeNS: function(node, ns, attribute) {
      var attr_value = node.getAttribute(ns + ':' + attribute);

      if (attr_value === undefined)
        attr_value = node.getAttributeNS(ns, attribute);

      if (attr_value === undefined)
        attr_value = node.getAttribute(attribute);

      return attr_value;
    },
    enforceType: function(type, value) {

      switch (type) {
        case 'boolean':
          value = (value === 'true');
          break;

        case 'integer':
        case 'long':
        case 'float':
        case 'double':
          value = +value;
          break;
      }

      return value;
    },
    getRGB: function(values) {
      return (values[3]) ?
        'rgba(' + values.join(',') + ')' :
        'rgb(' + values.slice(0, -1).join(',') + ')';
    }
  };


  /**
   * Parser Core Functions
   * ----------------------
   *
   * The XML parser's functions themselves.
   */

  /**
   * Node structure.
   * A function returning an object guarded with default value.
   *
   * @param  {object} properties The node properties.
   * @return {object}            The guarded node object.
   */
  function Node(properties) {

    // Possible Properties
    return {
      id: properties.id,
      label: properties.label,
      attributes: properties.attributes || {},
      viz: properties.viz || {}
    };
  }


  /**
   * Edge structure.
   * A function returning an object guarded with default value.
   *
   * @param  {object} properties The edge properties.
   * @return {object}            The guarded edge object.
   */
  function Edge(properties) {

    // Possible Properties
    return {
      id: properties.id,
      type: properties.type || 'undirected',
      label: properties.label || '',
      source: properties.source,
      target: properties.target,
      weight: +properties.weight || 1.0,
      viz: properties.viz || {}
    };
  }

  /**
   * Graph parser.
   * This structure parse a gexf string and return an object containing the
   * parsed graph.
   *
   * @param  {string} xml The xml string of the gexf file to parse.
   * @return {object}     The parsed graph.
   */
  function Graph(xml) {
    var _xml = {};

    // Basic Properties
    //------------------
    _xml.els = {
      root: xml.getElementsByTagName('gexf')[0],
      graph: xml.getElementsByTagName('graph')[0],
      meta: xml.getElementsByTagName('meta')[0],
      model: xml.getElementsByTagName('attribute'),
      nodes: xml.getElementsByTagName('node'),
      edges: xml.getElementsByTagName('edge')
    };

    _xml.hasViz = !!_helpers.getAttributeNS(_xml.els.root, 'xmlns', 'viz');
    _xml.version = _xml.els.root.getAttribute('version') || '1.0';
    _xml.mode = _xml.els.graph.getAttribute('mode') || 'static';

    var edgeType = _xml.els.graph.getAttribute('defaultedgetype');
    _xml.defaultEdgetype = edgeType || 'undirected';


    // Parser Functions
    //------------------

    // Meta Data
    function _metaData() {

      var metas = {};
      if (!_xml.els.meta)
        return metas;

      // Last modified date
      metas.lastmodifieddate = _xml.els.meta.getAttribute('lastmodifieddate');

      // Other information
      _helpers.nodeListEach(_xml.els.meta.childNodes, function(child) {
        metas[child.tagName.toLowerCase()] = child.textContent;
      });

      return metas;
    }

    // Model
    function _model() {
      var attributes = [];

      // Iterating through attributes
      _helpers.nodeListEach(_xml.els.model, function(attr) {

        // Properties
        var properties = {
          id: attr.getAttribute('id') || attr.getAttribute('for'),
          type: attr.getAttribute('type') || 'string',
          title: attr.getAttribute('title') || ''
        };

        // Defaults
        var default_el = _helpers.nodeListToArray(attr.childNodes);

        if (default_el.length > 0)
          properties.defaultValue = default_el[0].textContent;

        // Creating attribute
        attributes.push(properties);
      });

      return attributes;
    }

    // Nodes
    function _nodes(model) {
      var nodes = [];

      // Iteration through nodes
      _helpers.nodeListEach(_xml.els.nodes, function(n) {

        // Basic properties
        var properties = {
          id: n.getAttribute('id'),
          label: n.getAttribute('label') || ''
        };

        // Retrieving data from nodes if any
        if (model.length > 0)
          properties.attributes = _nodeData(model, n);

        // Retrieving viz information
        if (_xml.hasViz)
          properties.viz = _nodeViz(n);

        // Pushing node
        nodes.push(Node(properties));
      });

      return nodes;
    }

    // Data from nodes
    function _nodeData(model, node) {

      var data = {};
      var attvalues_els = node.getElementsByTagName('attvalue');

      // Getting Node Indicated Attributes
      var ah = _helpers.nodeListToHash(attvalues_els, function(el) {
        var attributes = _helpers.namedNodeMapToObject(el.attributes);
        var key = attributes.id || attributes['for'];

        // Returning object
        return {key: key, value: attributes.value};
      });


      // Iterating through model
      model.map(function(a) {

        // Default value?
        var att_title = a.title.toLowerCase();
        data[att_title] = !(a.id in ah) && 'defaultValue' in a ?
          _helpers.enforceType(a.type, a.defaultValue) :
          _helpers.enforceType(a.type, ah[a.id]);

      });

      return data;
    }

    // Viz information from nodes
    function _nodeViz(node) {
      var viz = {};

      // Color
      var color_el = _helpers.getFirstElementByTagNS(node, 'viz', 'color');

      if (color_el) {
        var color = ['r', 'g', 'b', 'a'].map(function(c) {
          return color_el.getAttribute(c);
        });

        viz.color = _helpers.getRGB(color);
      }

      // Position
      var pos_el = _helpers.getFirstElementByTagNS(node, 'viz', 'position');

      if (pos_el) {
        viz.position = {};

        ['x', 'y', 'z'].map(function(p) {
          viz.position[p] = +pos_el.getAttribute(p);
        });
      }

      // Size
      var size_el = _helpers.getFirstElementByTagNS(node, 'viz', 'size');
      if (size_el)
        viz.size = +size_el.getAttribute('value');

      // Shape
      var shape_el = _helpers.getFirstElementByTagNS(node, 'viz', 'shape');
      if (shape_el)
        viz.shape = shape_el.getAttribute('value');

      return viz;
    }

    // Edges
    function _edges(default_type) {
      var edges = [];

      // Iteration through edges
      _helpers.nodeListEach(_xml.els.edges, function(e) {

        // Creating the edge
        var properties = _helpers.namedNodeMapToObject(e.attributes);
        if (!('type' in properties)) {
          properties.type = default_type;
        }

        // Retrieving viz information
        if (_xml.hasViz)
          properties.viz = _edgeViz(e);

        edges.push(Edge(properties));
      });

      return edges;
    }

    // Viz information from edges
    function _edgeViz(edge) {
      var viz = {};

      // Color
      var color_el = _helpers.getFirstElementByTagNS(edge, 'viz', 'color');

      if (color_el) {
        var color = ['r', 'g', 'b', 'a'].map(function(c) {
          return color_el.getAttribute(c);
        });

        viz.color = _helpers.getRGB(color);
      }

      // Shape
      var shape_el = _helpers.getFirstElementByTagNS(edge, 'viz', 'shape');
      if (shape_el)
        viz.shape = shape_el.getAttribute('value');

      // Thickness
      var thick_el = _helpers.getFirstElementByTagNS(edge, 'viz', 'thickness');
      if (thick_el)
        viz.thickness = +thick_el.getAttribute('value');

      return viz;
    }


    // Returning the Graph
    //---------------------
    _xml.model = _model();

    return {
      version: _xml.version,
      mode: _xml.mode,
      defaultEdgeType: _xml.defaultEdgetype,
      meta: _metaData(),
      model: _xml.model,
      nodes: _nodes(_xml.model),
      edges: _edges(_xml.defaultEdgetype)
    };
  }


  /**
   * Public API
   * -----------
   *
   * User-accessible functions.
   */

  // Fetching GEXF with XHR
  function fetch(gexf_url, callback) {
    var xhr = (function() {
      if (window.XMLHttpRequest)
        return new XMLHttpRequest();

      var names,
          i;

      if (window.ActiveXObject) {
        names = [
          'Msxml2.XMLHTTP.6.0',
          'Msxml2.XMLHTTP.3.0',
          'Msxml2.XMLHTTP',
          'Microsoft.XMLHTTP'
        ];

        for (i in names)
          try {
            return new ActiveXObject(names[i]);
          } catch (e) {}
      }

      return null;
    })();

    if (!xhr)
      throw 'XMLHttpRequest not supported, cannot load the file.';

    // Async?
    var async = (typeof callback === 'function'),
        getResult;

    // If we can't override MIME type, we are on IE 9
    // We'll be parsing the response string then.
    if (xhr.overrideMimeType) {
      xhr.overrideMimeType('text/xml');
      getResult = function(r) {
        return r.responseXML;
      };
    }
    else {
      getResult = function(r) {
        var p = new DOMParser();
        return p.parseFromString(r.responseText, 'application/xml');
      };
    }

    xhr.open('GET', gexf_url, async);

    if (async)
      xhr.onreadystatechange = function() {
        if (xhr.readyState === 4)
          callback(getResult(xhr));
      };

    xhr.send();

    return (async) ? xhr : getResult(xhr);
  }

  // Parsing the GEXF File
  function parse(gexf) {
    return Graph(gexf);
  }

  // Fetch and parse the GEXF File
  function fetchAndParse(gexf_url, callback) {
    if (typeof callback === 'function') {
      return fetch(gexf_url, function(gexf) {
        callback(Graph(gexf));
      });
    } else
      return Graph(fetch(gexf_url));
  }


  /**
   * Exporting
   * ----------
   */
  this.GexfParser = {

    // Functions
    parse: parse,
    fetch: fetchAndParse,

    // Version
    version: '0.1'
  };

}).call(this);
</script>
"""

// COMMAND ----------

val htmlTop = """<html xmlns="https://www.w3.org/1999/xhtml">"""
val head = """
<head>
  <title>GEXF D3</title>
  <meta charset="utf-8" />
  """ + style + 
"""
</head>
"""
val htmlBody = """
<body onload="loadGraph">
<div id="vizcontainer" style="width:100%;height:100%">
<svg id="graphSVG" style="border:1px lightgray solid;">
  <g id="graphG" />
<div id="modal"><div id="content"></div><button id="modalClose" onclick="nodeFocus=false;nodeOut();d3.select('#modal').style('display','none');">X</button></div>
</div>
<div id="controls">
</div>
"""

// COMMAND ----------

val htmlFooter = "<footer>" + 
parser + d3GEFX + script + 
" </footer> "
val htmlTail = "</html>"

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Merged view

// COMMAND ----------

val view = htmlTop + head + htmlBody + htmlFooter + htmlTail

// COMMAND ----------

displayHTML(view)

// COMMAND ----------



// COMMAND ----------

