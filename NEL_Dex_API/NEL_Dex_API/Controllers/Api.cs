using NEL.NNS.lib;
using NEL_Dex_API.RPC;
using NEL_Dex_API.Service;
using Newtonsoft.Json.Linq;
using System;

namespace NEL_Dex_API.Controllers
{
    public class Api
    {
        private string netnode { get; set; }
        // services
        private DexService dexService;

        //
        private HttpHelper hh = new HttpHelper();
        private MongoHelper mh = new MongoHelper();
        //
        private static Api testApi = new Api("testnet");
        private static Api mainApi = new Api("mainnet");
        public static Api getTestApi() { return testApi; }
        public static Api getMainApi() { return mainApi; }
        private Monitor monitor;

        public Api(string node) {
            netnode = node;
            switch (netnode)
            {
                case "testnet":
                    dexService = new DexService
                    {
                        mh = mh,
                        Notify_mongodbConnStr = mh.notify_mongodbConnStr_testnet,
                        Notify_mongodbDatabase = mh.notify_mongodbDatabase_testnet,
                        dexContractHash = mh.dexContractHash_testnet,
                        dexBalanceStateCol = mh.dexBalanceStateCol_testnet,
                        dexDomainSellStateCol = mh.dexDomainSellStateCol_testnet,
                        dexDomainBuyStateCol = mh.dexDomainBuyStateCol_testnet,
                        dexDomainDealHistStateCol = mh.dexDomainDealHistStateCol_testnet,
                        domainOwnerCol = mh.domainOwnerCol_testnet,
                        auctionStateCol = mh.auctionStateCol_testnet
                    };
                    break;
                case "mainnet":
                    dexService = new DexService
                    {
                        mh = mh,
                        Notify_mongodbConnStr = mh.notify_mongodbConnStr_mainnet,
                        Notify_mongodbDatabase = mh.notify_mongodbDatabase_mainnet,
                        dexContractHash = mh.dexContractHash_mainnet,
                        dexBalanceStateCol = mh.dexBalanceStateCol_mainnet,
                        dexDomainSellStateCol = mh.dexDomainSellStateCol_mainnet,
                        dexDomainBuyStateCol = mh.dexDomainBuyStateCol_mainnet,
                        dexDomainDealHistStateCol = mh.dexDomainDealHistStateCol_mainnet,
                        domainOwnerCol = mh.domainOwnerCol_mainnet,
                        auctionStateCol = mh.auctionStateCol_mainnet
                    };
                    break;
            }

            initMonitor();
        }

        public object getRes(JsonRPCrequest req, string reqAddr)
        {
            JArray result = new JArray();
            string resultStr = string.Empty;
            string findFliter = string.Empty;
            string sortStr = string.Empty;
            try
            {
                point(req.method);
                switch (req.method)
                {
                    // dex
                    case "verifyEmail":
                        result = dexService.verifyEmail(req.@params[0].ToString(), req.@params[1].ToString(), req.@params[2].ToString());
                        break;
                    case "clearEmail":
                        result = dexService.clearEmail(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "bindEmail":
                        result = dexService.bindEmail(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "getEmailState":
                        result = dexService.getEmailState(req.@params[0].ToString());
                        break;
                    case "hasStarDomain":
                        result = dexService.hasStarDomain(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "getStarDomainList":
                        result = dexService.getStarDomainList(req.@params[0].ToString(), int.Parse(req.@params[1].ToString()), int.Parse(req.@params[2].ToString()));
                        break;
                    case "getStarDomainCount":
                        result = dexService.getStarDomainCount(req.@params[0].ToString());
                        break;
                    case "starDexDomain":
                        //result = dexService.starDexDomain(req.@params[0].ToString(), req.@params[1].ToString(), req.@params[2].ToString());
                        result = dexService.starDexDomain(req.@params[0].ToString(), int.Parse(req.@params[1].ToString()), req.@params[2].ToString(), req.@params[3].ToString());
                        break;
                    case "searchDexDomainLikeInfo":
                        result = dexService.searchDexDomainLikeInfo(req.@params[0].ToString(), req.@params[1].ToString(), int.Parse(req.@params[2].ToString()), int.Parse(req.@params[3].ToString()));
                        break;
                    case "searchDexDomainInfo":
                        result = dexService.searchDexDomainInfo(req.@params[0].ToString());
                        break;
                    case "getOrderRange":
                        result = dexService.getOrderRange(decimal.Parse(req.@params[0].ToString()));
                        break;
                    case "getDexDomainList":
                        result = dexService.getDexDomainList(req.@params[0].ToString(), req.@params[1].ToString(), req.@params[2].ToString(), req.@params[3].ToString(), int.Parse(req.@params[4].ToString()), int.Parse(req.@params[5].ToString()));
                        break;
                    case "getDexDomainCanUseList":
                        result = dexService.getDexDomainCanUseList(req.@params[0].ToString(), req.@params[1].ToString(), int.Parse(req.@params[2].ToString()), int.Parse(req.@params[3].ToString()));
                        break;
                    case "getDexDomainInfo":
                        result = dexService.getDexDomainInfo(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "getDexDomainOrder":
                        result = dexService.getDexDomainOrder(req.@params[0].ToString(), req.@params[1].ToString(), int.Parse(req.@params[2].ToString()), int.Parse(req.@params[3].ToString()));
                        break;
                    case "getDexDomainBuyOther":
                        result = dexService.getDexDomainBuyOther(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "getDexDomainBuyDetail":
                        result = dexService.getDexDomainBuyDetail(req.@params[0].ToString());
                        break;
                    case "getDexDomainSellOther":
                        result = dexService.getDexDomainSellOther(req.@params[0].ToString());
                        break;
                    case "getDexDomainSellDetail":
                        result = dexService.getDexDomainSellDetail(req.@params[0].ToString());
                        break;
                    case "getDexDomainDealHistList":
                        result = dexService.getDexDomainDealHistList(req.@params[0].ToString(), int.Parse(req.@params[1].ToString()), int.Parse(req.@params[2].ToString()), req.@params[3].ToString(), req.@params[4].ToString());
                        break;
                    case "getDexDomainBuyList":
                        result = dexService.getDexDomainBuyList(req.@params[0].ToString(), int.Parse(req.@params[1].ToString()), int.Parse(req.@params[2].ToString()), req.@params[3].ToString(), req.@params[4].ToString(), req.@params[5].ToString());
                        break;
                    case "getDexDomainSellList":
                        result = dexService.getDexDomainSellList(req.@params[0].ToString(), int.Parse(req.@params[1].ToString()), int.Parse(req.@params[2].ToString()), req.@params[3].ToString(), req.@params[4].ToString(), req.@params[5].ToString());
                        break;
                    case "getBalanceFromDex":
                        result = dexService.getBalanceFromDex(req.@params[0].ToString());
                        break;
                    
                    // test
                    case "getnodetype":
                        result = new JArray { new JObject { { "nodeType", netnode } } };
                        break;
                }
                if (result.Count == 0)
                {
                    JsonPRCresponse_Error resE = new JsonPRCresponse_Error(req.id, -1, "No Data", "Data does not exist");
                    return resE;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("errMsg:{0},errStack:{1}", e.Message, e.StackTrace);
                JsonPRCresponse_Error resE = new JsonPRCresponse_Error(req.id, -100, "Parameter Error", e.Message);
                return resE;
            }

            JsonPRCresponse res = new JsonPRCresponse();
            res.jsonrpc = req.jsonrpc;
            res.id = req.id;
            res.result = result;

            return res;
        }

        private void initMonitor()
        {
            string startMonitorFlag = mh.startMonitorFlag;
            if (startMonitorFlag == "1")
            {
                monitor = new Monitor();
            }
        }
        private void point(string method)
        {
            if (monitor != null)
            {
                monitor.point(netnode, method);
            }
        }
    }
}
