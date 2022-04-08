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
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Hash "mo:base/Hash";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat8 "mo:base/Nat8";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import String "mo:base/Text";
import Text "mo:base/Text";
import Candy "mo:candy/types";


module {

    type Hash = Hash.Hash;
    type Result<T,E> = Result.Result<T,E>;

    public type PipeInstanceID = Hash.Hash;

    public type AddressedChunk = Candy.AddressedChunk;
    public type Workspace = Candy.Workspace;

    public type DataChunk = Buffer.Buffer<Nat8>;
    public type DataZone = Buffer.Buffer<DataChunk>;




    //pipeline types
    public type PipelinifyIntitialization = {
        onDataWillBeLoaded: ?((Hash, ?ProcessRequest) -> PipelineEventResponse);
        onDataReady: ?((Hash, Workspace, ?ProcessRequest) -> PipelineEventResponse);
        onPreProcess: ?((Hash, Workspace, ?ProcessRequest, ?Nat) -> PipelineEventResponse);
        onProcess: ?((Hash, Workspace, ?ProcessRequest, ?Nat) -> PipelineEventResponse);
        onPostProcess: ?((Hash, Workspace, ?ProcessRequest, ?Nat) -> PipelineEventResponse);
        onDataWillBeReturned: ?((Hash, Workspace,?ProcessRequest) -> PipelineEventResponse);
        onDataReturned: ?((Hash, ?ProcessRequest, ?ProcessResponse) -> PipelineEventResponse);
        getProcessType: ?((Hash, Workspace, ?ProcessRequest) -> ProcessType);
        getLocalWorkspace: ?((Hash, Nat, ?ProcessRequest) -> Candy.Workspace);
        putLocalWorkspace: ?((Hash, Nat, Candy.Workspace, ?ProcessRequest) -> Candy.Workspace);

    };

    public type ChunkResponse = {
        #chunk: [AddressedChunk];
        #eof: [AddressedChunk];
        #parallel: (Nat, Nat,[AddressedChunk]);
        #err: ProcessError;
    };

    public type ChunkPush = {
        pipeInstanceID: PipeInstanceID;
        chunk: ChunkResponse;
    };

    public type StepRequest = {
        pipeInstanceID: PipeInstanceID;
        step: ?Nat;
    };

    public type DataConfig  = {
        #dataIncluded : {
            data: [AddressedChunk]; //data if small enough to fit in the message
        };
        #local : Nat;

        #pull : {
            sourceActor: ?DataSource;
            sourceIdentifier: ?Hash.Hash;
            mode : { #pull; #pullQuery;};
            totalChunks: ?Nat32;
            data: ?[AddressedChunk];
        };
        #push;
        #internal;
    };


    public type ProcessRequest = {
        event: ?Text;
        dataConfig: DataConfig;
        processConfig: ?Candy.CandyValue;
        executionConfig: ExecutionConfig;
        responseConfig: ResponseConfig;
    };

    public type RequestCache = {
        request : ProcessRequest;
        timestamp : Nat;
        status: {
            #initialized;
            #dataDone;
            #responseReady;
            #finalized;
        };
    };

    public type ProcessCache = {
        map : [var Bool];
        steps : Nat;
        var status: {
            #initialized;
            #done;
            #pending: Nat;
        };
    };

    public type ExecutionConfig = {
        executionMode: {
                #onLoad;
                #manual;
            };
    };

    public type ResponseConfig = {
        responseMode: {
                #push;
                #pull;
                #local : Nat;
            };
    };

    public type ProcessError = {
        text: Text;
        code: Nat;
    };


    public type ProcessResponse = {
        #dataIncluded: {
            payload: [AddressedChunk];
        };
        #local : Nat;
        #intakeNeeded: {
            pipeInstanceID: PipeInstanceID;
            currentChunks: Nat;
            totalChunks: Nat;
            chunkMap: [Bool];
        };
        #outtakeNeeded: {
            pipeInstanceID: PipeInstanceID;
        };
        #stepProcess: {
            pipeInstanceID: PipeInstanceID;
            status: ProcessType;
        };

    };

    public type DataReadyResponse = {
        #dataIncluded: {
            payload: [AddressedChunk];
        };
        #error: {
            text: Text;
            code: Nat;
        };
    };

    public type PipelineEventResponse = {
        #dataNoOp;
        #dataUpdated;
        #stepNeeded;
        #error : ProcessError;
    };

    public type WorkspaceCache = {
        var status : {
            #initialized;
            #loading: (Nat,Nat,[Bool]); //(chunks we've seen, totalChunks, map of recieved items)
            #doneLoading;
            #processing: Nat;
            #doneProcessing;
            #returning: Nat;
            #done
        };
        data: Workspace;
    };



    public type ChunkRequest = {
        chunkID: Nat;
        event: ?Text;
        sourceIdentifier: ?Hash.Hash;
    };

    public type ChunkGet = {
        chunkID: Nat;
        chunkSize: Nat;
        pipeInstanceID: PipeInstanceID;
    };

    public type PushStatusRequest = {
        pipeInstanceID: PipeInstanceID;
    };
    public type ProcessingStatusRequest = {
        pipeInstanceID: PipeInstanceID;
    };

    public type DataSource = actor {
        requestPipelinifyChunk : (_request : ChunkRequest) -> async Result<ChunkResponse,ProcessError>;
        queryPipelinifyChunk : query (_request : ChunkRequest) -> async Result<ChunkResponse,ProcessError>;
    };

    public type ProcessActor = actor {
        process : (_request : ProcessRequest) -> async Result<ProcessResponse,ProcessError>;
        getChunk : (_request : ChunkGet) -> async Result<ChunkResponse,ProcessError>;
        pushChunk: (_request: ChunkPush) -> async Result<ProcessResponse,ProcessError>;
        getPushStatus: query (_request: PushStatusRequest) -> async Result<ProcessResponse,ProcessError>;
        getProcessingStatus: query (_request: ProcessingStatusRequest) -> async Result<ProcessResponse,ProcessError>;
        singleStep: (_request: StepRequest) -> async Result<ProcessResponse,ProcessError>;
    };


    public type PushToPipeStatus = {#pending : Nat; #finished; #err: ProcessError;};
    public type ProcessingStatus = {#pending : (Nat, Nat, {#parallel;#sequential;}); #finished; #err: ProcessError;};

    public type ProcessType = {
        #unconfigured;
        #error;
        #sequential: Nat;
        #parallel: {
            stepMap: [Bool];
            steps: Nat;
        };

    };






    //error list
    //
    // Intake push errors
    // 8 - cannot find intake cache for provided id
    // 9 - Cannot add to a 'done' intake cache
    // 10 - Do not send an error chunk type to the intake process
    // 11 - Unloaded Intake Cache. Should Not Be Here.
    // 12 - Request cache is missing.
    // 16 - Do not send an error chunk

    // GetChunk errors
    // 13 -- cannot find response cache

    //pushChunk
    // 14 -- parallel - pipe already pushed
    // 15 -- done loading

    //pushChunk Status
    //17 -- Pipe is not in intake state

    //single step
    //19 -- error with step
    //20 -- map missing
    //21 -- processing not ready
    //22 -- done processing



};
