using RabbitMQ.Client;
using System;
using System.Text;

namespace Send
{
    class Program
    {
        static void Main(string[] args)
        {
            RpcClient rpcClient = new RpcClient();
            var response = rpcClient.Call("40");
            Console.WriteLine($"得到回傳：{response}");
            rpcClient.Close();
            Console.ReadLine();
        }

        /// <summary>
        /// 消息確認和持久化
        /// </summary>
        public static void MessageAcknowledgmentAndDurability()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // 要設定屬性Persistent = true
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    // 要將durable設定為true，才能有消息持久化
                    channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, null);
                    while (1 == 1)
                    {
                        string message = Console.ReadLine();
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish("", "task_queue", properties, body);
                    }

                }
            }
        }

        /// <summary>
        /// 此種方式沒有宣告柱列，接收方可以透過交換器取得，柱列用臨時的就好
        /// </summary>
        public static void ExchangeSetting()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // logs是自訂義交換器名稱，type的Fanout模式，是會將有連接到的通通傳送訊息出去
                channel.ExchangeDeclare(exchange:"logs", type: ExchangeType.Fanout);
                //無限迴圈執行輸入資料
                while (1 == 1)
                {
                    string message = Console.ReadLine();
                    var body = Encoding.UTF8.GetBytes(message);
                    // 會透過交換器發布出去
                    channel.BasicPublish("logs", "", null, body);
                }
            }
        }

        public static void ExchangeAndRoutingBind()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "direct_logs", type: ExchangeType.Direct);

                while (1 == 1)
                {
                    //輸入路由和訊息
                    Console.Write("請輸入路由和訊息(請用空格區分，空格前是路由，空格後是訊息)：");
                    var enterValue = Console.ReadLine();
                    var splValue = enterValue.Split(' ');
                    // 取得路由和訊息
                    var routingKey = splValue[0];
                    var message = splValue[1];

                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "direct_logs", routingKey: routingKey, basicProperties: null, body: body);
                }
            }
        }

        public static void TopicExchange()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // 宣告一個交換器，並設定為topic
                channel.ExchangeDeclare(exchange: "topic_logs", type: ExchangeType.Topic);


                while (1 == 1)
                {
                    //輸入路由和訊息
                    Console.Write("請輸入路由和訊息(請用空格區分，空格前是路由，空格後是訊息)：");
                    var enterValue = Console.ReadLine();
                    var splValue = enterValue.Split(' ');
                    // 取得路由和訊息
                    var routingKey = splValue[0];
                    var message = splValue[1];

                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "topic_logs", routingKey: routingKey, basicProperties: null, body: body);
                }
            }
        }
    }
}
