namespace TableStore.Interface
{
    using System.ServiceModel;

    public static class ServiceBindings
    {
        public static NetTcpBinding TcpBinding { get; private set; }

        static ServiceBindings()
        {
            TcpBinding = new NetTcpBinding(SecurityMode.None);
        }
    }
}
