namespace TableStore.Client.Utilities
{
    using System.ServiceModel;
    using System.Threading.Tasks;

    internal static class ChannelFactoryExtensions
    {
        public static Task OpenAsync(this ChannelFactory channelFactory)
        {
            return Task.Factory.FromAsync(channelFactory.BeginOpen, channelFactory.EndOpen, null);
        }

        public static Task CloseAsync(this ChannelFactory channelFactory)
        {
            return Task.Factory.FromAsync(channelFactory.BeginClose, channelFactory.EndClose, null);
        }
    }
}
