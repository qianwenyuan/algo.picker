module.exports = function(RED) {
    function kmeansModelNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
	if(!config.k){
		this.warn('cluster number k is not specified.');
	}
    }
    RED.nodes.registerType("kmeans-model",kmeansModelNode);
}

