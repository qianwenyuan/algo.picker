module.exports = function(RED) {
    function filterNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
	if(!config.condition){
		this.warn('condition not specified.');
	}
    }
    RED.nodes.registerType("filter",filterNode);
}

