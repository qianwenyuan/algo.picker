/**
 * Copyright 2014, 2016 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

var log;
var redNodes;
var settings;
//modified by sladezhang
var request = require('request');

module.exports = {
    init: function(runtime) {
        settings = runtime.settings;
        redNodes = runtime.nodes;
        log = runtime.log;
    },
    get: function(req,res) {
        var version = req.get("Node-RED-API-Version")||"v1";
        if (version === "v1") {
            log.audit({event: "flows.get",version:"v1"},req);
            res.json(redNodes.getFlows().flows);
        } else if (version === "v2") {
            log.audit({event: "flows.get",version:"v2"},req);
            res.json(redNodes.getFlows());
        } else {
            log.audit({event: "flows.get",version:version,error:"invalid_api_version"},req);
            res.status(400).json({code:"invalid_api_version", message:"Invalid API Version requested"});
        }
    },
    post: function(req,res) {
        var version = req.get("Node-RED-API-Version")||"v1";
        var flows = req.body;
        var deploymentType = req.get("Node-RED-Deployment-Type")||"full";
        log.audit({event: "flows.set",type:deploymentType,version:version},req);
        if (deploymentType === 'reload') {
            redNodes.loadFlows().then(function() {
                res.status(204).end();
            }).otherwise(function(err) {
                log.warn(log._("api.flows.error-reload",{message:err.message}));
                log.warn(err.stack);
                res.status(500).json({error:"unexpected_error", message:err.message});
            });
        } else {
            var flowConfig = flows;
            if (version === "v2") {
                flowConfig = flows.flows;
                if (flows.hasOwnProperty('rev')) {
                    var currentVersion = redNodes.getFlows().rev;
                    if (currentVersion !== flows.rev) {
                        //TODO: log warning
                        return res.status(409).json({code:"version_mismatch"});
                    }
                }
            } else if (version !== 'v1') {
                log.audit({event: "flows.set",version:version,error:"invalid_api_version"},req);
                res.status(400).json({code:"invalid_api_version", message:"Invalid API Version requested"});
            }
            redNodes.setFlows(flowConfig,deploymentType).then(function(flowId) {
                if (version === "v1") {
                    res.status(204).end();
                } else if (version === "v2") {
		   //modified by sladezhang
                  console.log("Node posting to Spring Server");
		  console.log(JSON.stringify(flows.flows)); 
		  request({
			url:'http://localhost:8080', 
			method:'POST',
			body:JSON.stringify(flows.flows)
			}, 
			function(error, response, body){
				if(!error && response.statusCode == 200){
					res.json({rev:flowId, SpringRespon: body});
				}else{
					res.json({rev:flowId, SpringError: error});
				}
		    	});
		   // res.json({rev:flowId});
                } else {
                    // TODO: invalid version
                }
            }).otherwise(function(err) {
                log.warn(log._("api.flows.error-save",{message:err.message}));
                log.warn(err.stack);
                res.status(500).json({error:"unexpected_error", message:err.message});
            });
        }
    }
}
