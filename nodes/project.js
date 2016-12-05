module.exports = function(RED) {
    function projectNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
	if(!config.projections){
		this.warn('projections not specified.');
	}
    }
    RED.nodes.registerType("project",projectNode);
}

