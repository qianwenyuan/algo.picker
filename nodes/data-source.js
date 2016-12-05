module.exports = function(RED) {
    function dataSourceNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
	if(!config.name){
		this.warn('data source missing name');
	}
    }
    RED.nodes.registerType("data-source",dataSourceNode);
}

