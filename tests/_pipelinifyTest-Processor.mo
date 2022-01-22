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
import Candy "mo:candy";
import Hash "mo:base/Hash";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Time "mo:base/Time";
import Nat8 "mo:base/Nat8";

actor class Processor(){
    type Result<T,E> = Result.Result<T,E>;

    var nonce : Nat = 0;

    type Hash = Hash.Hash;

    func onProcess(_hash : Hash, _data : PipelinifyTypes.Workspace, _processRequest : ?PipelinifyTypes.ProcessRequest, _step : ?Nat) : PipelinifyTypes.PipelineEventResponse {

        //Debug.print("processing chunk" # debug_show(_processRequest));
        switch(_processRequest){
            case(?_processRequest){


                switch(_processRequest.dataConfig, _processRequest.event){
                    case(#dataIncluded(data), ?event){
                        if(event == "dataIncludedTest"){
                            Debug.print("In the test");
                            //Debug.print(debug_show( _data.get(0).get(0).toArray()));
                            if(Array.equal([0:Nat8,1:Nat8,2:Nat8,3:Nat8], Candy.valueUnstableToBytes(_data.get(0).get(0)), Nat8.equal)){
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(0,4:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(1,5:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(2,6:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(3,7:Nat8);
                                Debug.print("done updating");
                                return #dataUpdated;
                            } else {
                                //Debug.print(debug_show( _data.get(0).get(0).toArray()));
                                return #error({text = "Not Implemented"; code = 99999});
                            };
                        };

                    };
                    case(#pull(data), ?event){
                        if (event == "dataPullTest") {
                            //Debug.print("should have data" # debug_show(_data.get(0).get(0).toArray()));
                            if(Array.equal([0:Nat8,0:Nat8,2:Nat8,2:Nat8], Candy.valueUnstableToBytes(_data.get(0).get(0)), Nat8.equal)){
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(0,3:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(1,3:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(2,4:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(3,4:Nat8);
                                return #dataUpdated;
                            } else {
                                return #error({text = "Not Implemented"; code = 99999});
                            };
                        };
                        if (event == "dataPullTestChunk") {
                            //Debug.print("should have data from chunks" # debug_show(_data.get(0).get(0)));
                            let __data = Candy.valueUnstableToBytes(_data.get(0).get(7));
                            if(Array.equal([8:Nat8,8:Nat8,8:Nat8,8:Nat8], [__data[0],__data[1],__data[2],__data[3]], Nat8.equal)){
                                Debug.print("returning 8,8,8,8");
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(7)).put(0,8:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(7)).put(1,8:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(7)).put(2,8:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(7)).put(3,8:Nat8);
                                return #dataUpdated;
                            } else {
                                Debug.print("returning error");
                                return #error({text = "Not Implemented"; code = 99999});
                            };
                        };
                        if (event == "dataPullTestChunkUnknown") {
                            //Debug.print("should have data from unknown chunks" # debug_show(_data.get(0).get(0)));
                            let __data = Candy.valueUnstableToBytes(_data.get(0).get(5));
                            if(Array.equal([6:Nat8,6:Nat8,6:Nat8,6:Nat8], [__data[0],__data[1],__data[2],__data[3]], Nat8.equal)){
                                Debug.print("returning 6,6,6,6");
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(0,6:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(1,6:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(2,6:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(3,6:Nat8);
                                return #dataUpdated;
                            } else {
                                Debug.print("returning error");
                                return #error({text = "Not Implemented"; code = 99999});
                            };
                        };
                        if (event == "dataPullQueryFull") {
                            //Debug.print("should have data from query" # debug_show(_data.get(0).get(0)));
                            let __data = Candy.valueUnstableToBytes(_data.get(0).get(0));
                            if(Array.equal([32:Nat8,33:Nat8,34:Nat8,35:Nat8], [__data[0],__data[1],__data[2],__data[3]], Nat8.equal)){
                                Debug.print("returning 22,23,24,25");
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(0,22:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(1,23:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(2,24:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(3,25:Nat8);
                                return #dataUpdated;
                            } else {
                                Debug.print("returning error");
                                return #error({text = "Not Implemented"; code = 99999});
                            };
                        }

                    };
                    case(#push, ?event){
                        if (event == "dataPush") {
                            //Debug.print("should have data" # debug_show(_data.get(0).get(0)));
                            if(Array.equal([10:Nat8,9:Nat8,8:Nat8,7:Nat8], Candy.valueUnstableToBytes(_data.get(0).get(0)), Nat8.equal)){
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(0,5:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(1,4:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(2,3:Nat8);
                                Candy.valueUnstableToBytesBuffer(_data.get(0).get(0)).put(3,2:Nat8);
                                return #dataUpdated;
                            } else {
                                return #error({text = "Not Implemented"; code = 99999});
                            };
                        };
                    };

                    case(_, _){
                        return #error({text = "Not Implemented"; code = 99999});
                    }
                };

            };
            case(null){
                return #error({text = "process request null"; code = 999991});
            };
        };

        return #error({text = "process request null"; code = 999991});
    };



    let pipelinify = Pipelinify.Pipelinify({
        onDataWillBeLoaded = null;
        onDataReady = null;
        onPreProcess = null;
        onProcess = ?onProcess;
        onPostProcess = null;
        onDataWillBeReturned = null;
        onDataReturned = null;
        getProcessType = null;
        getLocalWorkspace = null;
        putLocalWorkspace = null;
    });


    public func process(_request : PipelinifyTypes.ProcessRequest) : async Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError>{
       return await pipelinify.process(_request);
    };

    public func pushChunk(_chunk : PipelinifyTypes.ChunkPush) : async Result<PipelinifyTypes.ProcessResponse, PipelinifyTypes.ProcessError>{
       return await pipelinify.pushChunk(_chunk);
    };
};
