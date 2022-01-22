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

import Array "mo:base/Array";
import Nat "mo:base/Nat";
import Pipelinify "../src";
import PipelinifyTypes "../src/types";
import Principal "mo:base/Principal";
import Hash "mo:base/Hash";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Time "mo:base/Time";
import Nat8 "mo:base/Nat8";
import Text "mo:base/Text";
import Processor "_pipelinifyTest-Processor";


shared (install) actor class Consumer() = this {

    type Result<T,E> = Result.Result<T,E>;

    //var nonce : Nat = 0;

    type Hash = Hash.Hash;
    //let thisPrincipal : Principal = Principal.fromActor(this);
    //let thisChunkHandler : PipelinifyTypes.DataSource = this;

    let processor : Processor.Processor = actor("ryjl3-tyaaa-aaaaa-aaaba-cai");

    public func requestPipelinifyChunk(_request : PipelinifyTypes.ChunkRequest) : async Result<PipelinifyTypes.ChunkResponse, PipelinifyTypes.ProcessError>{
        //Debug.print("Chunk being provided" # debug_show(_request));
        switch(_request.sourceIdentifier){
            case(?sourceIdentifier){
                if(sourceIdentifier == Text.hash("dataPullTest")){
                    Debug.print("Returning 0,0,2,2");
                    return #ok(#chunk([(0,0,#Bytes(#thawed([0:Nat8,0:Nat8,2:Nat8,2:Nat8])))]));
                };
                if(sourceIdentifier == Text.hash("dataPullTestChunk")){
                    Debug.print("Returning chunk");
                    return #ok(#chunk([(0,_request.chunkID,#Bytes(#thawed([Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1)])))]));
                };
                if(sourceIdentifier == Text.hash("dataPullTestChunkUnknown")){
                    Debug.print("Returning chunk");
                    if(_request.chunkID < 5){
                        return #ok(#chunk([(0,_request.chunkID,#Bytes(#thawed([Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1)])))]));
                    } else if (_request.chunkID == 5){
                        return #ok(#eof([(0,_request.chunkID,#Bytes(#thawed([Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1),Nat8.fromNat(_request.chunkID + 1)])))]));
                    } else if (_request.chunkID > 5){
                        return #ok(#eof([(0,_request.chunkID,#Bytes(#thawed([])))]));
                    };
                };
            };
            case(_){
                return #err{text="Not Implemented"; code = 99899897;};
            };
        };
        return #err{text="Not Implemented"; code = 99899898;};
    };

    public query func queryPipelinifyChunk(_request : PipelinifyTypes.ChunkRequest) : async Result<PipelinifyTypes.ChunkResponse, PipelinifyTypes.ProcessError>{

        Debug.print("Chunk being provided" # debug_show(_request));
        switch(_request.sourceIdentifier){
            case(?sourceIdentifier){
                if(sourceIdentifier == Text.hash("dataPullQueryFull")){
                    Debug.print("Returning 32,33,34,35");
                    return #ok(#chunk([(0,0,#Bytes(#thawed([32:Nat8,33:Nat8,34:Nat8,35:Nat8])))]));
                };
            };

            case(_){
                return #err{text="Not Implemented"; code = 99899897;};
            };
        };
        return #err{text="Not Implemented"; code = 99899898;};
    };


    public func testFullDataSendProcess(_starterArray: [PipelinifyTypes.AddressedChunk]) : async [PipelinifyTypes.AddressedChunk] {


        Debug.print("==========testFullDataSendProcess==========");




        let response = await processor.process({
            event = ?"dataIncludedTest";
            dataConfig = #dataIncluded{
                data : [PipelinifyTypes.AddressedChunk] = _starterArray;

            };
            executionConfig = {executionMode = #onLoad};
            responseConfig = {responseMode = #push};
            processConfig = null;
        });

        switch(response){
            case(#ok(data)){
                switch(data){
                    case(#dataIncluded(details)){
                        return details.payload;
                    };
                    case(_){
                        return [];
                    };
                };
            };
            case(_){
                return [];
            };
        };


        return [];
    };


    public func testPullFullProcess() : async [PipelinifyTypes.AddressedChunk] {


        Debug.print("==========testPullFullProcess==========");



        let response = await processor.process({
            event = ?"dataPullTest";
            dataConfig = #pull{
                sourceActor = ?this;

                sourceIdentifier = ?Text.hash("dataPullTest");
                mode = #pull;
                totalChunks = ?1;
                data = null;
            };
            executionConfig = {executionMode = #onLoad};
            responseConfig = {responseMode = #push};
            processConfig = null;
        });

        switch(response){
            case(#ok(data)){
                switch(data){
                    case(#dataIncluded(details)){
                        return details.payload;
                    };
                    case(_){
                        return [];
                    };
                };
            };
            case(_){
                return [];
            };
        };


        return [];
    };

    public func testPullChunkProcess() : async [PipelinifyTypes.AddressedChunk] {


        Debug.print("==========testPullChunkProcess==========");



        let response = await processor.process({
            event = ?"dataPullTestChunk";
            dataConfig = #pull{
                sourceActor = ?this;

                sourceIdentifier = ?Text.hash("dataPullTestChunk");
                mode = #pull;
                totalChunks = ?8;
                data = ?[(0,0,#Bytes(#thawed([1,1,1,1])))];
            };
            executionConfig = {executionMode = #onLoad};
            responseConfig = {responseMode = #push};
            processConfig = null;
        });

        Debug.print(debug_show(response));

        switch(response){
            case(#ok(data)){
                switch(data){
                    case(#dataIncluded(details)){
                        return details.payload;
                    };
                    case(_){
                        return [];
                    };
                };
            };
            case(_){
                return [];
            };
        };


        return [];
    };


    public func testPullChunkUnknownProcess() : async [PipelinifyTypes.AddressedChunk] {


        Debug.print("==========testPullChunkUnknownProcess==========");



        let response = await processor.process({
            event = ?"dataPullTestChunkUnknown";
            dataConfig = #pull{
                sourceActor = ?this;

                sourceIdentifier = ?Text.hash("dataPullTestChunkUnknown");
                mode = #pull;
                totalChunks = null;
                data = ?[(0,0,#Bytes(#thawed([1,1,1,1])))];
            };
            executionConfig = {executionMode = #onLoad};
            responseConfig = {responseMode = #push};
            processConfig = null;
        });

        Debug.print(debug_show(response));

        switch(response){
            case(#ok(data)){
                switch(data){
                    case(#dataIncluded(details)){
                        return details.payload;
                    };
                    case(_){
                        return [];
                    };
                };
            };
            case(_){
                return [];
            };
        };


        return [];
    };

    public func testPullFullQueryResponse() : async [PipelinifyTypes.AddressedChunk] {


        Debug.print("==========testPullFullQueryResponse==========");

        let response = await processor.process({
            event = ?"dataPullQueryFull";
            dataConfig = #pull{
                sourceActor = ?this;

                sourceIdentifier = ?Text.hash("dataPullQueryFull");
                mode = #pullQuery;
                totalChunks = ?1;
                data = null;
            };
            executionConfig = {executionMode = #onLoad};
            responseConfig = {responseMode = #push};
            processConfig = null;
        });

        Debug.print(debug_show(response));

        switch(response){
            case(#ok(data)){
                switch(data){
                    case(#dataIncluded(details)){
                        return details.payload;
                    };
                    case(_){
                        return [];
                    };
                };
            };
            case(_){
                return [];
            };
        };


        return [];
    };


    public func testPushFullResponse() : async [PipelinifyTypes.AddressedChunk] {


        Debug.print("==========testPushFullResponse==========");

        let response = await processor.process({
            event = ?"dataPush";
            dataConfig = #push;
            executionConfig = {executionMode = #onLoad};
            responseConfig = {responseMode = #push};
            processConfig = null;
        });

        Debug.print(debug_show(response));
        switch(response){
            case(#err(errType)){
                return [];
            };
            case(#ok(responseType)){
                switch(responseType){
                    case(#intakeNeeded(result)){
                        let pushFullResponse = await processor.pushChunk({
                            pipeInstanceID = result.pipeInstanceID;
                            chunk = #eof([(0,0,#Bytes(#thawed([10:Nat8,9:Nat8,8:Nat8,7:Nat8])))]);});

                        Debug.print("got a response from the push");
                        Debug.print(debug_show(pushFullResponse));

                        switch(pushFullResponse){
                            case(#ok(data)){
                                switch(data){
                                    case(#dataIncluded(details)){
                                        return details.payload;
                                    };
                                    case(_){
                                        return [];
                                    };
                                };
                            };
                            case(_){
                                return [];
                            };
                        };
                    };
                    case(_){
                        Debug.print("Did not find intake needed");
                        return [];
                    };
                };

            };
        };




        return [];
    };

};