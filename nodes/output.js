module.exports = function(RED) {
    function outputNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
    }
    RED.nodes.registerType("output",outputNode);
}

