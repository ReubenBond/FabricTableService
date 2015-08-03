namespace TableStore.Service.Utilities
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;

    internal class NetworkUtil
    {
        public static Task<IPAddress> GetIpAddress()
        {
            return GetIpAddress(Dns.GetHostName());
        }

        /// <summary>
        /// Returns the host's network address.
        /// </summary>
        /// <param name="host">
        /// The host.
        /// </param>
        /// <returns>
        /// The host's network address.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Unable to determine the host's network address.
        /// </exception>
        public static async Task<IPAddress> GetIpAddress(string host)
        {
            var nodeAddresses = await Dns.GetHostAddressesAsync(host);

            var nodeAddressV4 =
                nodeAddresses.FirstOrDefault(_ => _.AddressFamily == AddressFamily.InterNetwork && !IsLinkLocal(_));
            var nodeAddressV6 =
                nodeAddresses.FirstOrDefault(_ => _.AddressFamily == AddressFamily.InterNetworkV6 && !IsLinkLocal(_));
            var nodeAddress = nodeAddressV4 ?? nodeAddressV6;
            if (nodeAddress == null)
            {
                throw new InvalidOperationException("Could not determine own network address.");
            }

            return nodeAddress;
        }

        /// <summary>
        /// Returns <see langword="true"/> if the provided <paramref name="address"/> is a local-only address.
        /// </summary>
        /// <param name="address">The address.</param>
        /// <returns><see langword="true"/> if the provided <paramref name="address"/> is a local-only address.</returns>
        public static bool IsLinkLocal(IPAddress address)
        {
            if (address.AddressFamily == AddressFamily.InterNetworkV6)
            {
                return address.IsIPv6LinkLocal;
            }

            // 169.254.0.0/16
            var addrBytes = address.GetAddressBytes();
            return addrBytes[0] == 0xA9 && addrBytes[1] == 0xFE;
        }
    }
}
