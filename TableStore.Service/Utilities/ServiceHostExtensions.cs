namespace TableStore.Service.Utilities
{
    using System.ServiceModel;
    using System.Threading.Tasks;

    internal static class ServiceHostExtensions
    {
        public static Task OpenAsync(this ServiceHost host)
        {
            return Task.Factory.FromAsync(host.BeginOpen, host.EndOpen, null);
        }
        public static Task CloseAsync(this ServiceHost host)
        {
            return Task.Factory.FromAsync(host.BeginClose, host.EndClose, null);
        }
    }
}
