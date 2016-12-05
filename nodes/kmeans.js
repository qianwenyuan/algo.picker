module.exports = function(RED) {
    function kmeansNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        if(!config.model){
                this.warn('model not specified.');
        }
    }
    RED.nodes.registerType("kmeans",kmeansNode);
}
