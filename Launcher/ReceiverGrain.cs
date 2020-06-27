using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Launcher
{
    public interface IReceiverGrain : IGrainWithGuidKey
    {
        
    }

    public class MyObserver : IAsyncObserver<Tuple<int, int>>
    {
        public Task OnCompletedAsync()
        {
            Console.WriteLine("Hi OnCompletedAsync");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine($"In OnErrorAsync: {ex}");
            return Task.CompletedTask;
        }

        public Task OnNextAsync(Tuple<int, int> item, StreamSequenceToken token = null)
        {
            Console.WriteLine($"Received item, Our sequence is {item.Item1} and batch is {item.Item2}. Sequence Token is (SN:{token?.SequenceNumber}, EI:{token?.EventIndex})");
            return Task.CompletedTask;
        }
    }

    [ImplicitStreamSubscription("TESTDATA")]
    public class ReceiverGrain : Grain, IReceiverGrain
    {
        public override async Task OnActivateAsync()
        {
            //Create a GUID based on our GUID as a grain
            var guid = this.GetPrimaryKey();
            //Get one of the providers which we defined in config
            var streamProvider = GetStreamProvider("MemoryTest");
            //Get the reference to a stream
            var stream = streamProvider.GetStream<Tuple<int, int>>(guid, "TESTDATA");

            //Set our OnNext method to the lambda which simply prints the data, this doesn't make new subscriptions because we are using implicit subscriptions via [ImplicitStreamSubscription].

            await stream.SubscribeAsync<Tuple<int, int>>(async (item, token) => Console.WriteLine($"Received item, Our sequence is {item.Item1} and batch is {item.Item2}. Sequence Token is (SN:{token?.SequenceNumber}, EI:{token?.EventIndex})"));

            await base.OnActivateAsync();
        }
    }
}
