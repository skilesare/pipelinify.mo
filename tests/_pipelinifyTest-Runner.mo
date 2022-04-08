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
import Candy "mo:candy/types";
import Conversion "mo:candy/conversion";
import Hash "mo:base/Hash";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Time "mo:base/Time";
import Nat8 "mo:base/Nat8";
import Consumer "_pipelinifyTest-Consumer";

actor class pipelinify_runner(){
    type Result<T,E> = Result.Result<T,E>;

    var nonce : Nat = 0;

    type Hash = Hash.Hash;

    public func Test() : async Result<Text, Text>{
        /////////////////////////
        //      Data Load Test
        //        fullData - ✓ - Makes a request of the consumer, consuemer sends all data to the processor and gets a response.
        //        pullData(at Once) - ✓ - Makes a request of the consumer, consumer asks the processor to pull the data and then process
        //        pullData(chunks) - ✓ - Makes a request of the consumer, consumer asks the processor to pull 7 remaining chunks and then process
        //        pullDataUnkownNumberOfChunks - ✓ - Makes a request of the consumer, consumer asks the processor to pull remaining chunks ..consumer must respond with eof, and then processor process
        //        pullDataQuery ✓ - Makes a request of the consumer, consumer asks the processor to pull remaining chunks via query...uses same pathway as pull but switches to query
        //        push(delayed at once)  | push(chunks)
        //
        //           Execution Tests
        //           onLoad    |  selfServe(at once) | selfServe(step)  |  still processing  | aget(at once) | agent(step)
        //
        //           Data Retrieval Tests
        //           included  |  pullData(at once)  |  pullData(steps)  | pullDataQuery  |
        //////////////////////////

        var testStream : Text = "Running Tests \n";
        let consumer : Consumer.Consumer = actor("rrkah-fqaaa-aaaaa-aaaaq-cai");


        ////////
        //Full Data Test
        ////////

        testStream #= "Testing Full Data Send - ";

        let testFullDataSendResponse = await consumer.testFullDataSendProcess([(0,0,#Bytes(#thawed([0,1,2,3])))]);
        let tester5 = Conversion.valueToBytes(testFullDataSendResponse[0].2);

        if(tester5[0] == 4 and tester5[1] == 5 and tester5[2] == 6 and tester5[3] == 7){
            testStream #= "✓\n";
        }
        else{
            testStream #= "x\n";
            testStream #= "Expected " # debug_show(testFullDataSendResponse) # " to be [4,5,6,7]";
        };


        ////////
        //Pull Full Data
        ////////
        testStream #= "Testing Pull Full Data - ";

        let testPullFullResponse = await consumer.testPullFullProcess();
        let tester4 = Conversion.valueToBytes(testPullFullResponse[0].2);

        if(tester4[0] == 3 and tester4[1] == 3 and tester4[2] == 4 and tester4[3] == 4){
            testStream #= "✓\n";
        }
        else{
            testStream #= "x\n";
            testStream #= "Expected " # debug_show(testPullFullResponse) # " to be [3,3,4,4]";
        };

        ////////
        //Pull Chunked Data
        ////////
        testStream #= "Testing Pull Chunk Data - ";

        let testPullChunkResponse = await consumer.testPullChunkProcess();
        let tester3 = Conversion.valueToBytes(testPullChunkResponse[7].2);

        Debug.print(debug_show(testPullChunkResponse));

        if(tester3[0] == 8 and tester3[1] == 8 and tester3[2] == 8 and tester3[3] == 8){
            testStream #= "✓\n";
        }
        else{
            testStream #= "x\n";
            testStream #= "Expected " # debug_show(testPullChunkResponse) # " to be [8,8,8,8]";
        };

        ////////
        //Pull Chunked Data - unkown size
        ////////
        testStream #= "Testing Pull Chunk Data Unknown Size - ";

        let testPullChunkUnknownResponse = await consumer.testPullChunkUnknownProcess();
        let tester2 = Conversion.valueToBytes(testPullChunkUnknownResponse[0].2);

        if(tester2[0] == 6 and tester2[1] == 6 and tester2[2] == 6 and tester2[3] == 6){
            testStream #= "✓\n";
        }
        else{
            testStream #= "x\n";
            testStream #= "Expected " # debug_show(testPullChunkUnknownResponse) # " to be [6,6,6,6]";
        };

        ////////
        //Pull Full Data Via Query
        ////////
        testStream #= "Testing Pull Full Data via Query - ";

        let testPullFullQueryResponse = await consumer.testPullFullQueryResponse();
        let tester1 = Conversion.valueToBytes(testPullFullQueryResponse[0].2);


        if(tester1[0] == 22 and tester1[1] == 23 and tester1[2] == 24 and tester1[3] == 25){
            testStream #= "✓\n";
        }
        else{
            testStream #= "x\n";
            testStream #= "Expected " # debug_show(testPullFullQueryResponse) # " to be [22,23,26,25]";
        };

        ////////
        //Push Full Data
        ////////
        testStream #= "Testing Push Data - Full - ";

        let testPushFullResponse = await consumer.testPushFullResponse();
        let tester = Conversion.valueToBytes(testPushFullResponse[0].2);

        if(tester[0] == 5 and tester[1] == 4 and tester[2] == 3 and tester[3] == 2){
            testStream #= "✓\n";
        }
        else{
            testStream #= "x\n";
            testStream #= "Expected " # debug_show(testPushFullResponse) # " to be [5,4,3,2]";
        };




        return #ok(testStream);

    };


};
