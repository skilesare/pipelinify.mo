///////////////////////////////
//
// ©2021 RIVVIR Tech LLC
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
///////////////////////////////


import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import Result "mo:base/Result";
import PipelinifyTypes "types";
import Candy "mo:candy";
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Hash "mo:base/Hash";
import HashMap "mo:base/HashMap";
import Time "mo:base/Time";
import Int "mo:base/Int";

module {

    public class Pipelinify(_pipeline : PipelinifyTypes.PipelinifyIntitialization){
        type Result<T,E> = Result.Result<T,E>;

        type PipeInstanceID = Hash.Hash;

        var nonce : Nat = 0;


        func selfHash(_self : Hash.Hash) : Hash.Hash {
            _self;
        };

        let workspaceCache = HashMap.HashMap<Hash.Hash, PipelinifyTypes.WorkspaceCache>(
            16,
            Hash.equal,
            selfHash
        );

        let requestCache = HashMap.HashMap<Hash.Hash, PipelinifyTypes.RequestCache>(
            16,
            Hash.equal,
            selfHash
        );

        let processCache = HashMap.HashMap<Hash.Hash, PipelinifyTypes.ProcessCache>(
            16,
            Hash.equal,
            selfHash
        );

        func getPipeInstanceID(request: PipelinifyTypes.ProcessRequest) : PipeInstanceID {
            nonce += 1;
            //todo enable time or some randomish var for the nonce
            //Hash.hash(Int.abs(Time.now()) + nonce);

            let thisHash = Hash.hash(nonce);
            requestCache.put(thisHash, {
                request = {
                    event = request.event;
                    dataConfig = switch(request.dataConfig){
                        case(#dataIncluded(data)){#internal;};
                        case(#local(data)){#local(data);};
                        case(#pull(pullRequest)){
                            #pull{
                                sourceActor= pullRequest.sourceActor;
                                sourceIdentifier = pullRequest.sourceIdentifier;
                                mode = pullRequest.mode;
                                totalChunks = pullRequest.totalChunks;
                                data = ?[];
                            };
                        };
                        case(#push){#push};
                        case(#internal){#internal};
                    };
                    processConfig = request.processConfig;
                    executionConfig = request.executionConfig;
                    responseConfig = request.responseConfig;};
                timestamp = Int.abs(Time.now());
                status = #initialized;
            });

            thisHash
        };


        let onDataWillBeLoaded : (PipeInstanceID, ?PipelinifyTypes.ProcessRequest) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onDataWillBeLoaded){
            case(null){
                func _onDataReady(_hash : PipeInstanceID, _processRequest : ?PipelinifyTypes.ProcessRequest) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;
                }
            };
            case(?_onDataWillBeLoaded){
                _onDataWillBeLoaded;
            }
        };

        let onDataReady : (PipeInstanceID, PipelinifyTypes.Workspace, ?PipelinifyTypes.ProcessRequest) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onDataReady){
            case(null){
                func _onDataReady(_hash : PipeInstanceID, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;
                }
            };
            case(?_onDataReady){
                _onDataReady;
            }
        };

        let onPreProcess : (PipeInstanceID, PipelinifyTypes.Workspace, ?PipelinifyTypes.ProcessRequest, ?Nat) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onPreProcess){
            case(null){
                func _onPreProcess(_hash : PipeInstanceID, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest, _step : ?Nat) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;

                }
            };
            case(?_onPreProcess){
                _onPreProcess;
            }
        };

        let onProcess : (PipeInstanceID, PipelinifyTypes.Workspace, ?PipelinifyTypes.ProcessRequest, ?Nat) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onProcess){
            case(null){
                func _onProcess(_hash : PipeInstanceID, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest, _step : ?Nat) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;

                }
            };
            case(?_onProcess){
                _onProcess;
            }
        };

        let onPostProcess : (PipeInstanceID, PipelinifyTypes.Workspace, ?PipelinifyTypes.ProcessRequest, ?Nat) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onPostProcess){
            case(null){
                func _onPostProcess(_hash : PipeInstanceID, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest, _step: ?Nat) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;

                }
            };
            case(?_onPostProcess){
                _onPostProcess;
            }
        };

        let onDataWillBeReturned : (PipeInstanceID, PipelinifyTypes.Workspace, ?PipelinifyTypes.ProcessRequest) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onDataWillBeReturned){
            case(null){
                func _onDataWillBeReturned(_hash : PipeInstanceID, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;

                }
            };
            case(?_onDataWillBeReturned){
                _onDataWillBeReturned;
            }
        };

        let onDataReturned : (PipeInstanceID, ?PipelinifyTypes.ProcessRequest, ?PipelinifyTypes.ProcessResponse) -> PipelinifyTypes.PipelineEventResponse = switch (_pipeline.onDataReturned){
            case(null){
                func _onDataReturned(_hash : PipeInstanceID,  _processRequest : ?PipelinifyTypes.ProcessRequest, _processResponse : ?PipelinifyTypes.ProcessResponse) : PipelinifyTypes.PipelineEventResponse{
                    //just returns what it was given
                    return #dataNoOp;

                }
            };
            case(?_onDataReturned){
                _onDataReturned;
            }
        };

        let getProcessType : (PipeInstanceID, PipelinifyTypes.Workspace, ?PipelinifyTypes.ProcessRequest) -> PipelinifyTypes.ProcessType = switch (_pipeline.getProcessType){
            case(null){
                func _getProcessType(_hash : PipeInstanceID, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest) : PipelinifyTypes.ProcessType{
                    //just returns what it was given
                    return #unconfigured;

                }
            };
            case(?_getProcessType){
                _getProcessType;
            }
        };

        let getLocalWorkspace : (PipeInstanceID, Nat, ?PipelinifyTypes.ProcessRequest) -> Candy.Workspace = switch (_pipeline.getLocalWorkspace){
            case(null){
                func _getLocalWorkspace(_hash : PipeInstanceID, _id : Nat, _request: ?PipelinifyTypes.ProcessRequest) : Candy.Workspace{
                    //just returns what it was given
                    return Candy.emptyWorkspace();

                }
            };
            case(?_getLocalWorkspace){
                _getLocalWorkspace;
            }
        };

        let putLocalWorkspace : (PipeInstanceID, Nat, Candy.Workspace, ?PipelinifyTypes.ProcessRequest) -> Candy.Workspace = switch (_pipeline.putLocalWorkspace){
            case(null){
                func _putLocalWorkspacee(_hash : PipeInstanceID, _id : Nat, _workspace: Candy.Workspace, _request: ?PipelinifyTypes.ProcessRequest) : Candy.Workspace{
                    //just returns what it was given
                    return Candy.emptyWorkspace();

                }
            };
            case(?_putLocalWorkspace){
                _putLocalWorkspace;
            }
        };


        func handleProcessing(pipeInstanceID : Hash.Hash, data: PipelinifyTypes.Workspace, request : ?PipelinifyTypes.ProcessRequest, step: ?Nat) : Result<{data :PipelinifyTypes.Workspace; bFinished: Bool},PipelinifyTypes.ProcessError> {
            //process the data
            var _request : ?PipelinifyTypes.ProcessRequest = request;

            if(_request == null){
                let currentCache = requestCache.get(pipeInstanceID);
                _request := do?{currentCache!.request};
            };
            switch(_request){
                case(null){
                    //todo: return error
                };
                case(?_request){
                    switch(_request.executionConfig.executionMode){
                        case(#onLoad){
                            Debug.print("handling onLoad - Preprocess");
                            let preProcessResponse = onPreProcess(pipeInstanceID, data, ?_request, step);
                            //todo: handle response from preprocess
                            Debug.print("handling onLoad - process");
                            let response : PipelinifyTypes.PipelineEventResponse = onProcess(pipeInstanceID, data, ?_request, step);
                            //todo: handle response
                            switch(response){
                                case(#dataUpdated){
                                    Debug.print("setting processedData");
                                    //processedData := data.newData;
                                };
                                case(#stepNeeded){
                                    Debug.print("more processing needed a");
                                    return #ok{data=data;bFinished=false};
                                };
                                case(_){
                                    Debug.print("Not implemnted an error occured" # debug_show(response))
                                };
                            };
                            Debug.print("handling onLoad - postProcess");
                            let postProcessResponse = onPostProcess(pipeInstanceID, data, ?_request, step);
                            //todo: handle response
                            return #ok({data=data; bFinished=true});
                        };
                        case(#manual){
                            Debug.print("handling manual - Preprocess");
                            let preProcessResponse = onPreProcess(pipeInstanceID, data, ?_request, step);
                            //todo: handle response from preprocess
                            Debug.print("handling manual - process");
                            let response : PipelinifyTypes.PipelineEventResponse = onProcess(pipeInstanceID, data, ?_request, step);
                            //todo: handle response
                            switch(response){
                                case(#dataUpdated){
                                    Debug.print("setting processedData");
                                    //processedData := data.newData;
                                    //todo...we are returing false so that the client can manually finalize
                                    //todo probsbly need to calll on post prcess manually in the finalize function
                                    return #ok{data=data;bFinished=false};
                                };
                                case(#stepNeeded){
                                    Debug.print("more processing needed b");
                                    return #ok{data=data;bFinished=false};
                                };
                                case(_){
                                    Debug.print("Not implemnted an error occured" # debug_show(response))
                                };
                            };
                            Debug.print("handling manual - postProcess");
                            let postProcessResponse = onPostProcess(pipeInstanceID, data, ?_request, step);
                            //todo: handle response
                            return #ok({data=data; bFinished=true;});
                        };


                    };
                };
            };
            return #err({code=77777; text="Not Implemented Execution Pathway"});
        };

        func handleReturn(pipeInstanceID : Hash.Hash, finalData: PipelinifyTypes.Workspace, request : ?PipelinifyTypes.ProcessRequest) : Result<PipelinifyTypes.ProcessResponse,PipelinifyTypes.ProcessError> {
            //process the data
            var _request : ?PipelinifyTypes.ProcessRequest = request;

            if(_request == null){
                let currentCache = requestCache.get(pipeInstanceID);
                _request := do?{currentCache!.request};
            };
            switch(_request){
                case(null){
                    //todo: return error
                };
                case(?_request){
                    switch(_request.responseConfig.responseMode){
                        case(#push){
                            Debug.print("returning the data");
                            let dataWillBeReturnedResponse = onDataWillBeReturned(pipeInstanceID, finalData, ?_request);
                            let processResponse =
                                #dataIncluded{
                                    payload = Candy.workspaceToAddressedChunkArray(finalData);
                                };
                            let dataReturnedResponse : PipelinifyTypes.PipelineEventResponse = onDataReturned(pipeInstanceID, ?_request, ?processResponse);

                            //todo: dilema: how do we return before we call onData Return!
                            return #ok(processResponse);
                        };
                        case(#pull){
                            Debug.print("waiting to return data");
                            //let dataWillBeReturnedResponse = onDataWillBeReturned(pipeInstanceID, processedData, ?_request);
                            //responseCache.put(pipeInstanceID, {data = processedData} );
                            let processResponse =
                                #outtakeNeeded{
                                    pipeInstanceID = pipeInstanceID;
                                };
                            //let dataReturnedResponse : PipelinifyTypes.PipelineEventResponse = onDataReturned(pipeInstanceID, ?_request, ?processResponse);

                            //todo: dilema: how do we return before we call onData Return!
                            return #ok(processResponse);
                        };

                        case(#local(id)){
                            Debug.print("returning data locally");
                            let dataWillBeReturnedResponse = putLocalWorkspace(pipeInstanceID, id, finalData, ?_request);
                            //responseCache.put(pipeInstanceID, {data = processedData} );
                            let processResponse =
                                #local(id);
                            //let dataReturnedResponse : PipelinifyTypes.PipelineEventResponse = onDataReturned(pipeInstanceID, ?_request, ?processResponse);

                            //todo: dilema: how do we return before we call onData Return!
                            return #ok(processResponse);
                        };
                        //case(_){
                        //    Debug.print("not handled the returning of data")
                        //};

                    };
                };
            };
            return #err({code=77777; text="Not Implemented Execution Pathway"});
        };

        public func process(_request : PipelinifyTypes.ProcessRequest) : async Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError> {
            //file the request and alert to new data
            //assign a hash
            let pipeInstanceID = getPipeInstanceID(_request);

            Debug.print("the process is on");
            var thisWorkspace = Candy.emptyWorkspace();



            //check the data
            switch(_request.dataConfig){
                case(#local(_id)){
                    let dataWillLoadResponse = onDataWillBeLoaded(pipeInstanceID, ?_request);
                    //todo: chunk data into workspace

                    thisWorkspace := getLocalWorkspace(pipeInstanceID, _id, ?_request);
                    workspaceCache.put(pipeInstanceID, {
                                var status = #initialized;
                                data = thisWorkspace;
                            });
                    Debug.print("workspace included" # debug_show(thisWorkspace.size()));
                    let dataResponse : PipelinifyTypes.PipelineEventResponse = onDataReady(pipeInstanceID, thisWorkspace, ?_request);
                };
                case(#dataIncluded(dataIncludedRequest)){
                    //Debug.print("data included" # debug_show(dataIncludedRequest.data));
                    //finalData := dataIncludedRequest.data;
                    let dataWillLoadResponse = onDataWillBeLoaded(pipeInstanceID, ?_request);
                    //todo: chunk data into workspace

                    thisWorkspace := Candy.fromAddressedChunks(dataIncludedRequest.data);
                    Debug.print("workspace included" # debug_show(thisWorkspace.size()));
                    let dataResponse : PipelinifyTypes.PipelineEventResponse = onDataReady(pipeInstanceID, thisWorkspace, ?_request);
                };
                case(#pull(pullRequest)){
                    Debug.print("pump needed ");
                    var bInitilized : Bool = false;
                    var bLoading : Bool = true;
                    var chunkCount : Nat32 = 0;
                    var dataChunks : [PipelinifyTypes.AddressedChunk] = [];
                    switch(pullRequest.data){
                        case(null){
                            //todo: this may be able to be moved
                            Debug.print("initilizing cache with no data");
                            workspaceCache.put(pipeInstanceID, {
                                var status = #initialized;
                                data = Candy.emptyWorkspace();
                            });
                            bInitilized := true;
                        };
                        case(?data){
                            Debug.print("initilizing cache with data");
                            bInitilized := true;
                            bLoading := true;
                            Candy.fileAddressedChunks(thisWorkspace, data);
                            chunkCount := 1;
                        };
                    };

                    var thisChunk : Nat = Nat32.toNat(chunkCount);
                    var totalChunks : Nat = 99999;
                    switch (pullRequest.totalChunks){
                        case(?foundChunks){
                            totalChunks := Nat32.toNat(foundChunks);
                        };
                        case(_){};
                    };


                    switch(pullRequest.mode, pullRequest.sourceActor){
                        case(#pull, null){
                            return #err({
                                text = "sourcePrincipal is require for pull";
                                code = 1;});
                        };
                        case(#pullQuery, null){
                            return #err({
                                text = "sourcePrincipal is require for pullQuery";
                                code = 1;});
                        };
                        case(_, ?sourceActor){
                            //try to pull in the data
                            //todo: maybe we try query and catch and fall back?

                            Debug.print("Polling sourceActor for chunks " # debug_show(chunkCount));

                            label dataRetrieve while(thisChunk < totalChunks){
                                Debug.print("processing chunk" # debug_show(thisChunk));
                                let chunkResult  = switch(pullRequest.mode){
                                    case(#pull){
                                        await sourceActor.requestPipelinifyChunk({
                                        chunkID = thisChunk;
                                        event = _request.event;
                                        sourceIdentifier = pullRequest.sourceIdentifier;
                                        });
                                    };
                                    case(#pullQuery){
                                        await sourceActor.queryPipelinifyChunk({
                                        chunkID = thisChunk;
                                        event = _request.event;
                                        sourceIdentifier = pullRequest.sourceIdentifier;
                                        });
                                    };

                                };


                                switch(chunkResult){
                                    case(#ok(response)){
                                        switch response{
                                            case(#chunk(chunkData)){
                                                Debug.print("found some Data");
                                                //dataChunks := Array.append<PipelinifyTypes.AddressedChunk>(dataChunks, Array.make<PipelinifyTypes.AddressedChunk>(chunkData));
                                                Candy.fileAddressedChunks(thisWorkspace, chunkData);
                                                chunkCount := chunkCount + 1;
                                            };
                                            case(#eof(chunkData)){
                                                Debug.print("found some eof data");
                                                Candy.fileAddressedChunks(thisWorkspace, chunkData);
                                                chunkCount := chunkCount + 1;
                                                break dataRetrieve;
                                            };
                                            case(#parallel(chunkData)){
                                                Debug.print("found some parallel data");
                                                Candy.fileAddressedChunks(thisWorkspace, chunkData.2);
                                                chunkCount := chunkCount + 1;
                                                break dataRetrieve;
                                            };
                                            case(#err(error)){
                                                Debug.print("Found Error when requesting Chunk");
                                                return #err{text = error.text; code = error.code;};
                                            };
                                        };
                                    };
                                    case(#err(error)){
                                        Debug.print("Found Error when requesting Chunk");
                                        return #err{text = error.text; code = error.code;};
                                    };
                                };
                                thisChunk += 1;
                            };
                            Debug.print("have final data chunks" );
                            //todo: how to return
                            //finalData := Array.flatten<Nat8>(dataChunks);
                            Debug.print("have final data ");
                        };



                    };
                };
                case(#push){
                    //we are basically done here. We need the user to push us data so we can continue. We do need to send the cache key.
                    Debug.print("this is a push operation");
                    //todo: maybe we require initialization
                    workspaceCache.put(pipeInstanceID, {
                        var status = #initialized;
                        data = Candy.emptyWorkspace();
                    });
                    return #ok(#intakeNeeded{
                        pipeInstanceID = pipeInstanceID;
                        currentChunks = 0;
                        totalChunks = 0;
                        chunkMap = [];
                    });
                };
                case(#internal){
                    return #err{text = "Internal should not be used for initial request."; code = 178;};
                }
            };



            //process the data
            //todo: make sure we skip this for single steps that aren't a push procedure
            let processingResult = handleProcessing(pipeInstanceID, thisWorkspace, ?_request, null);
            switch(processingResult){
                case(#ok(data)){
                    Debug.print("push branch processing results");
                    if(handleParallelProcessStepResult(pipeInstanceID,null, data) == true){
                        //more processing needed

                        return #ok(#stepProcess{
                            pipeInstanceID = pipeInstanceID;
                            status = getProcessType(pipeInstanceID, thisWorkspace,?_request);
                        });
                    };
                };


                case(#err(theError)){

                    return #err(theError);
                }
            };


            //return the data

            return handleReturn(pipeInstanceID, thisWorkspace, ?_request);

        };

        func handleParallelProcessStepResult(pipeInstanceID : Hash.Hash, step: ?Nat, data : {data: PipelinifyTypes.Workspace; bFinished: Bool}) : Bool{
            Debug.print("in parallel proces handling" # debug_show(data.bFinished));

            //more processing needed
            Debug.print("looking for cache");
            var cache = processCache.get(pipeInstanceID);
            //switch(cache){
            //    case(null){
            //        cache := initilizeParallelProcessCache();
            //    };
            //};
            switch(cache, step){

                case(?cache, ?step){
                    Debug.print("manipulating cache");
                    cache.map[step] := true;
                    cache.status := #pending(step);
                    return true;
                };
                case(?cache, null){
                    //not implemented - a paralle process with unknown steps - shouldn't be here
                    Debug.print("Hit not implemented parallel with unknown steps");

                    return false;
                };
                case(_,_){//initialize? I think we assume this is initialized elsewhere
                    Debug.print("No Cache and No Step...check finished" # debug_show(data.bFinished));
                    if(data.bFinished == false){

                        return true;
                    };
                    return false;
                };
            };
            return false;

        };

        public func singleStep(_request : PipelinifyTypes.StepRequest) : async Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError> {
           //process the data
           let thisCache = workspaceCache.get(_request.pipeInstanceID);
           let thisRequestCache = requestCache.get(_request.pipeInstanceID);



           Debug.print("In single step pipelinify " # debug_show(_request));
            switch(thisCache, thisRequestCache){

                case(?thisCache, ?thisRequestCache){
                    //let thisProcessStatus = getProcessType(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);

                    Debug.print("processing final chunks " # debug_show(thisCache.data.size()));
                    //var finalData = Array.flatten<Nat8>(thisCache.data);
                    //Debug.print("have final data single step " # debug_show(finalData.size()));
                    let processingResult = handleProcessing(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request, _request.step);
                    switch(processingResult){
                        case(#ok(data)){
                            Debug.print("handling single step process");
                            //processedData := data;
                            let notFinished : Bool = handleParallelProcessStepResult(_request.pipeInstanceID, _request.step, data);
                                //more processing needed
                            let status = getProcessType(_request.pipeInstanceID, thisCache.data,?thisRequestCache.request);
                            switch(status){
                                //let user finalize for parallel
                                case(#parallel(info)){
                                    Debug.print("inside of the more steps handler");
                                    return #ok(#stepProcess{
                                        pipeInstanceID = _request.pipeInstanceID;
                                        status = status;
                                    });
                                };
                                case(#sequential(info)){
                                    if(data.bFinished == true){
                                        //do nothing
                                    } else {
                                        return #ok(#stepProcess{
                                        pipeInstanceID = _request.pipeInstanceID;
                                        status = status;
                                    });
                                    };
                                };
                                case(_){
                                    return #err({code=948484; text="not configured"});
                                }
                            }
                        };
                        case(#err(theError)){

                            return #err(theError);
                        }
                    };


                    //return the data
                    Debug.print("shouldn only be here for sequential - return after single step");

                    return handleReturn(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);
                };
                case (_, _){
                    return #err({text="Cannot find intake Cache for ID" # Nat32.toText(_request.pipeInstanceID); code= 8;});
                };
            }


        };

        public func getPushStatus(_request : PipelinifyTypes.PushStatusRequest) : Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError> {
            let thisCache  = workspaceCache.get(_request.pipeInstanceID);
            let thisRequestCache  = requestCache.get(_request.pipeInstanceID);
            switch(thisCache, thisRequestCache){

                case(?thisCache, ?thisRequestCache){
                    switch(thisCache.status){
                        case(#initialized){
                            return #ok(#intakeNeeded{
                                        pipeInstanceID = _request.pipeInstanceID;
                                        currentChunks = 0;
                                        totalChunks = 0;
                                        chunkMap = [];
                                    });
                        };
                        case(#doneLoading){
                            return #ok(#stepProcess({
                                pipeInstanceID = _request.pipeInstanceID;
                                status = getProcessType(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);
                            }))};
                        case(#loading(chunkData)){
                            return #ok(#intakeNeeded{
                                        pipeInstanceID = _request.pipeInstanceID;
                                        currentChunks = chunkData.0;
                                        totalChunks = chunkData.1;
                                        chunkMap = chunkData.2;
                                    });
                        };
                        case(_){
                            return #err({code=15; text="done loading"});
                        }
                    }
                };
                case (_,_){
                    return #err({text="Cannot find intake Cache for ID" # Nat32.toText(_request.pipeInstanceID); code= 8;});
                };
            };
        };

        public func pushChunk(_request : PipelinifyTypes.ChunkPush) : async Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError> {

            // pull the intake cache
            let thisCache  = workspaceCache.get(_request.pipeInstanceID);
            switch(thisCache){
                case (null){
                    return #err({text="Cannot find intake Cache for ID" # Nat32.toText(_request.pipeInstanceID); code= 8;});
                };
                case(?thisCache){
                    //prepare the new cache entry
                    switch(thisCache.status){

                        case(#doneLoading){return #err({code=15; text="done loading";})};
                        case(#processing(val)){return #err({code=15; text="done loading";})};
                        case(#doneProcessing){return #err({code=15; text="done loading";})};
                        case(#returning(val)){return #err({code=15; text="done loading";})};
                        case(#done){return #err({code=15; text="done loading";})};
                        case(_){
                            //keep going
                            Debug.print("keep going");
                        };

                    };
                    switch(_request.chunk){
                        case(#chunk(chunkData)){

                                Debug.print("chunk pushed");
                                //Debug.print("done");

                                 switch(thisCache.status){
                                    case(#initialized){
                                        thisCache.status := #loading(1, 1, [true]);
                                    };
                                    case(#loading(loadingVal)){
                                        thisCache.status := #loading(loadingVal.0 + 1,loadingVal.1 + 1,Array.freeze<Bool>(Array.init<Bool>(loadingVal.0 + 1, true)));
                                    };

                                    case(_){
                                        //todo: need to handle errors
                                        //throw #err({text="Cannot add to intake cached for a 'done' intake cache"; code = 9;});
                                        return #err({code=15; text="done loading"});
                                        //Debug.print("done");
                                        //#done;
                                    };
                                };

                                //Debug.print("chunk data " # debug_show(chunkData[1]) # debug_show(chunkData[2]) # debug_show(chunkData[3]) # debug_show(chunkData[4]) # debug_show(chunkData[5]));
                                Candy.fileAddressedChunks(thisCache.data, chunkData);
                            };






                        case(#eof(chunkData)){
                                Debug.print("hit EOF Chunk");
                                thisCache.status := #doneLoading;
                                //Debug.print("eof data " # debug_show(chunkData[1]) # debug_show(chunkData[2]) # debug_show(chunkData[3]) # debug_show(chunkData[4]) # debug_show(chunkData[5]) );
                                Candy.fileAddressedChunks(thisCache.data, chunkData);

                        };
                        case(#parallel(chunkData)){
                            switch(thisCache.status){
                                case(#initialized){
                                    Debug.print("in parallel initialized");
                                    let thisMap = Array.init<Bool>(chunkData.1,false);
                                    thisMap[chunkData.0] := true;
                                    thisCache.status := #loading(chunkData.0, chunkData.1, Array.freeze<Bool>(thisMap));
                                    Candy.fileAddressedChunks(thisCache.data, chunkData.2);
                                    return #ok(#intakeNeeded{
                                        pipeInstanceID = _request.pipeInstanceID;
                                        currentChunks = chunkData.0;
                                        totalChunks = chunkData.1;
                                        chunkMap = Array.freeze<Bool>(thisMap);
                                    });

                                };
                                case(#loading(loadingVal)){
                                    Debug.print("in prallel loading");
                                    let thisMap = Array.thaw<Bool>(loadingVal.2);
                                    thisMap[chunkData.0] := true;
                                    thisCache.status := #loading(chunkData.0,chunkData.1, Array.freeze<Bool>(thisMap));
                                    Candy.fileAddressedChunks(thisCache.data, chunkData.2);
                                    Debug.print("map is " # debug_show(thisMap));
                                    return #ok(#intakeNeeded{
                                        pipeInstanceID = _request.pipeInstanceID;
                                        currentChunks = chunkData.0;
                                        totalChunks = chunkData.1;
                                        chunkMap = Array.freeze<Bool>(thisMap);
                                    });
                                };
                                case(_){
                                    //todo: need to handle errors
                                    //throw #err({text="Cannot add to intake cached for a 'done' intake cache"; code = 9;});
                                    Debug.print("error...already loaded");
                                    return #err({code = 14; text="pipe already pushed"});
                                };
                            };





                        };
                        case(#err(theErr)){
                            //todo...this should probably be an error 8..not sure how to return it.
                            return #err({code = 16; text="do not send an error chunk"});

                        };
                    };


                    //load the data into the cache
                    //todo: maybe only if not done?
                    //Debug.print("Putting the intake cache.");
                    //intakeCache.put(_request.pipeInstanceID, newCache);


                    switch(thisCache.status){
                        case(#initialized){
                            return #err({text="Unloaded Intake Cache. Should Not Be Here."; code = 11;})
                        };
                        case(#loading(loadingValue)){
                            //we are done and will hang out until the next cache push
                            Debug.print("Done...hanging out for more push.");
                            return #ok(#intakeNeeded{
                                pipeInstanceID = _request.pipeInstanceID;
                                currentChunks = loadingValue.0;
                                totalChunks = loadingValue.1;
                                chunkMap = loadingValue.2;
                            });
                        };
                        case(#doneLoading){
                            //we need to look at the execution config
                            Debug.print("we uploaded a chunk and now we are done");
                            let thisRequestCache = requestCache.get(_request.pipeInstanceID);
                            switch(thisRequestCache){
                                case(null){
                                    return #err({text="Request Cache is missing."; code = 12;});
                                };
                                case(?thisRequestCache){
                                    switch(thisRequestCache.request.executionConfig.executionMode){
                                        case(#onLoad){
                                            //we are going to run the process now

                                            Debug.print("have final data chunks");
                                            //var finalData = Array.flatten<Nat8>(newCache.data);
                                            //Debug.print("have final data " # debug_show(finalData.size()));

                                            let processingResult = handleProcessing(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request, null);

                                            Debug.print("got the results ");
                                            switch(processingResult){
                                                case(#ok(data)){
                                                    Debug.print("got back from the handle process");
                                                    if(handleParallelProcessStepResult(_request.pipeInstanceID, null, data) == true){
                                                        //more processing needed

                                                        return #ok(#stepProcess{
                                                            pipeInstanceID = _request.pipeInstanceID;
                                                            status = getProcessType(_request.pipeInstanceID, thisCache.data,?thisRequestCache.request);
                                                        });
                                                    };
                                                    //processedData := data;
                                                };
                                                case(#err(theError)){

                                                    Debug.print("I don't like that im here" );
                                                    return #err(theError);
                                                }
                                            };

                                            return handleReturn(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);

                                        };
                                        case(#manual){
                                            return #ok(#stepProcess{
                                                pipeInstanceID = _request.pipeInstanceID;
                                                status = getProcessType(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);

                                                });
                                        };
                                    };
                                };
                            };
                        };
                        case(_){
                            Debug.print("didnt handle status");
                        }
                    };
                };

            };


            //determine EOF

            //call process if necessary
            return #err({text="Not Implemented"; code = 984585;});
        };

        public func initilizeParallelProcessCache(_pipeInstanceID : PipelinifyTypes.PipeInstanceID, _steps: Nat) : PipelinifyTypes.ProcessCache {
            let thisCache : PipelinifyTypes.ProcessCache =  {
                steps = _steps;
                map = Array.init<Bool>(_steps, false);
                var status = #initialized;
            };
            processCache.put(_pipeInstanceID, thisCache);
            return thisCache;
        };

        public func getProcessCache(_pipeInstanceID : PipelinifyTypes.PipeInstanceID) : ?PipelinifyTypes.ProcessCache {

            let thisCache = processCache.get(_pipeInstanceID);

            return thisCache;
        };

        public func getProcessingStatus(_request : PipelinifyTypes.ProcessingStatusRequest) : Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError> {
            let thisCache  = workspaceCache.get(_request.pipeInstanceID);
            let thisRequestCache  = requestCache.get(_request.pipeInstanceID);
            switch(thisCache, thisRequestCache){

                case(?thisCache, ?thisRequestCache){
                    switch(thisCache.status){
                        case(#initialized){
                            return #err({code=21; text="processing not ready"});
                        };
                        case(#loading(someData)){
                            if(someData.2.size() > 0){
                                let total = someData.2.size();
                                var tracker = 0;
                                for(thisItem in someData.2.vals()){
                                    if(thisItem == true){
                                        tracker += 1;
                                    };
                                };
                                if(tracker < total){
                                    return #err({code=21; text="processing not ready"});
                                } else {
                                    return #ok(#stepProcess({
                                        pipeInstanceID = _request.pipeInstanceID;
                                        status = getProcessType(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);
                                    }));
                                };
                            };

                            return #err({code=21; text="processing not ready"});
                        };

                        case(#doneLoading){
                            return #ok(#stepProcess({
                                pipeInstanceID = _request.pipeInstanceID;
                                status = getProcessType(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);
                            }))};
                        case(#processing(chunkData)){
                            return #ok(#stepProcess({
                                pipeInstanceID = _request.pipeInstanceID;
                                status = getProcessType(_request.pipeInstanceID, thisCache.data, ?thisRequestCache.request);
                            }))};

                        case(#doneProcessing){
                            return #ok(#outtakeNeeded({
                            pipeInstanceID = _request.pipeInstanceID;
                            //todo: add outtake status
                        }))};

                        case(_){
                            return #err({code=22; text="done loading"});
                        }
                    }
                };
                case (_,_){
                    return #err({text="Cannot find intake Cache for ID" # Nat32.toText(_request.pipeInstanceID); code= 8;});
                };
            };
        };

        public func getChunk(_request : PipelinifyTypes.ChunkGet) : Result<PipelinifyTypes.ChunkResponse, PipelinifyTypes.ProcessError> {
            //return #err({code=99999999;text="not imlemented"});

            // pull the intake cache
            Debug.print("made it to get chunk");
            let thisCache = workspaceCache.get(_request.pipeInstanceID);

            switch(thisCache){
                case (null){
                    return #err({text="Cannot find response Cache for ID" # Nat32.toText(_request.pipeInstanceID); code= 13;});
                };
                case(?thisCache){
                    let result = Candy.getWorkspaceChunk(thisCache.data, _request.chunkID, _request.chunkSize);
                    switch(result.0){
                        case(#eof){
                            return #ok(#eof(result.1.toArray()));
                        };
                        case(#chunk){
                            return #ok(#chunk(result.1.toArray()));
                        };
                    };
                };
            };
            //call process if necessary
            return #err({text="Not Implemented"; code = 984585;});

        };

    };



};
