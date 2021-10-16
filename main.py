# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import datetime
from dataclasses import dataclass
import re
import ipaddress


@dataclass
class Provide:
    timeTook: datetime
    timeStarted: datetime
    timeFinished: datetime
    requestID: str


@dataclass
class FindProviders:
    timeStarted: datetime
    agentN: int
    timeTook: datetime
    timeFinished: datetime
    requestID: str
    peersFound: [str]


@dataclass
class Key:
    index: int
    keyHash: str
    agentN: int
    provides: {Provide}
    finds: {FindProviders}


@dataclass
class GetClosestPeersLookup:
    requestID: str  # may be null
    agentID: str
    peerContacted: str
    peerTargeted: str
    nPeersReceived: int


@dataclass
class Dial:
    dialID: str
    requestID: str
    connected: bool
    connectionSuccessful: bool  # only if the one before was false
    peerID: str
    agentID: str
    timeTook: float
    connectionsToPeer: [str]
    queryID: str


@dataclass
class Query:
    requestID: str
    queryID: str
    peerID: str
    agentID: str
    key: str
    timeStarted: datetime
    timeFinished: datetime
    timeTook: datetime


@dataclass
class GetProviders:
    requestID: str
    messageID: str
    peerID: str
    agentID: str
    key: str
    timeStarted: datetime
    timeFinished: datetime
    timeTook: datetime


@dataclass
class GetClosestPeers:
    requestID: str
    messageID: str
    agentID: str
    peerTargeted: str
    timeStarted: datetime
    timeFinished: datetime
    timeTook: datetime
    peerContacted: str
    nPeersReceived: int


@dataclass
class PutProvider:
    peerID: str
    key: str


log1 = open("./../Measurements/new/Modified-agent1/logs/log.txt")
log2 = open("./../Measurements/new/Modified-agent2/logs/log.txt")
log3 = open("./../Measurements/new/Modified-agent3/logs/log.txt")
log4 = open("./../Measurements/new/Modified-agent4/logs/log.txt")
log5 = open("./../Measurements/new/Modified-agent5/logs/log.txt")
logs = [log1, log2, log3, log4, log5]

err1 = open("./../Measurements/new/Modified-agent1/err.txt")
err2 = open("./../Measurements/new/Modified-agent2/err.txt")
err3 = open("./../Measurements/new/Modified-agent3/err.txt")
err4 = open("./../Measurements/new/Modified-agent4/err.txt")
err5 = open("./../Measurements/new/Modified-agent5/err.txt")
errs = [err1, err2, err3, err4, err5]
queries = {}
dials = {}
getClosestPeersLookup = []
getClosestPeers = {}
getProvs = {}
keys = [None] * 256
agentIDs = []

def timeTookConvert(t):
    if t.__contains__("h"):
        print("time com horas!!!")
    elif re.match(r"\d+m\d*", t):  # tem minutos
        # preciso de calcular quantos digitos tem os milisegundos pq o datetime so aceita com 6
        t = t.removesuffix("s")
        aux = t.split(".")
        t = t[:(6 - len(aux[2]))]
        return datetime.datetime.strptime(t, "%-Mm%-S.%f")
    elif re.match(r"\d+.\d*s", t):
        t = t.removesuffix("s")
        aux = t.split(".")
        t = t[:(6 - len(aux[2]))]
        return datetime.datetime.strptime(t, "%-S.%f")
    else:
        t = t.removesuffix("ms")
        t = t.replace('.', '')
        t = t[0:6]
        return datetime.datetime.strptime(t, "%f")


def timestampConvert(d):
    d = d.split(" +0000 UTC")[0]
    d = d[0:15]
    return datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f")


def process():
    # logs normais
    for i in range(0, 255):
        keys[i] = Key(index=i, agentN=i % 5, keyHash="", finds={}, provides={})
    count = 0
    currGet = None
    for f in logs:
        for l in f.readlines():
            l.removeprefix("{\"level\":\"info\",\"main\":\"")
            l.removesuffix("\"}")
            if l.__contains__("ID in the DHT"):
                aux = l.split("ID in the DHT is ")
                agentIDs[count] = aux[1]
            elif l.__contains__("Providing key"):
                aux = l.split(" key is ")
                ind = aux[0].removeprefix("Providing key with index ")
                aux = aux[1].split(" ID is ")
                reqID = aux[1]
                p = Provide(timeTook=None, timeFinished=None, timeStarted=None, requestID=reqID)
                keys[ind].provides[reqID] = p
            elif l.__contains__("Providing value with"):
                aux = l.split(" of index ")
                aux = aux[1].split(" request ID is ")
                ind = aux[0]
                aux = aux[1].split(" peer ID from key is ")
                reqID = aux[0]
                aux = aux[1].split(" at time ")
                k = aux[0]
                timeStart = timestampConvert(aux[1])
                keys[ind].keyHash = k
                keys[ind].provides[reqID].timeStarted = timeStart
            elif l.__contains__("Successful in provid"):
                aux = l.split(" of index ")
                aux = aux[1].split(" request ID is ")
                ind = aux[0]
                aux = aux[1].split(" peer ID from key is ")
                reqID = aux[0]
                aux = aux[1].split(" took ")
                # k = aux[0]
                aux = aux[1].split(" at time ")
                tTook = timeTookConvert(aux[0])
                timeFinish = timestampConvert(aux[1])
                keys[ind].provides[reqID].timeFinished = timeFinish
                keys[ind].provides[reqID].timeTook = tTook
            elif l.__contains__("Finding providers"):
                aux = l.split(" of index ")
                aux = aux[1].split(" ID is ")
                ind = aux[0]
                aux = aux[1].split(" peer ID from key is ")
                reqID = aux[0]
                aux = aux[1].split(" at time ")
                k = aux[0]
                timeStart = timestampConvert(aux[1])
                currGet = FindProviders(timeTook=None, timeFinished=None, timeStarted=timeStart,
                                                       requestID=reqID, agentN=count)
                keys[ind].finds[reqID] = currGet
            elif l.__contains__("Successful in finding"):
                aux = l.split(" of index ")
                aux = aux[1].split(" ID is ")
                ind = aux[0]
                aux = aux[1].split(" peer ID from key is ")
                reqID = aux[0]
                aux = aux[1].split(" took ")
                # k = aux[0]
                aux = aux[1].split(" at time ")
                tTook = timeTookConvert(aux[0])
                timeFinish = timestampConvert(aux[1])
                keys[ind].finds[reqID].timeFinished = timeFinish
                keys[ind].finds[reqID].timeTook = tTook
            elif l.__contains__("Length of val"):
                aux = l.split("; values are")
                lenVal = int(aux[0])
                vals = aux[1].split(" ")
                for v in vals:
                    currGet.peersFound.append(v)
        count += 1

    # erros
    for f in errs:
        for l in f.readlines():
            if l.__contains__("dial"):
                d = None
                if l.__contains__("query"):  # dialPeerMod
                    aux = l.split(" query ID: ")  # aux1 ID: .... aux2: <queryID> e mais
                    reqID = aux[0].removeprefix("ID: ")
                    aux = aux[1].split(" dial ID: ")
                    qID = aux[0]  # <queryID> // <dialID>..
                    aux = aux[1].split(" ")  # <dialID> // <mensagem para o if>...
                    dID = aux[0]
                    if dials.__contains__(dID):
                        d = dials[dID]
                        assert (d.requestID == reqID)
                        assert (d.queryID == qID)
                        assert (d.dialID == dID)
                    else:
                        d = Dial(dialID=dID, requestID=reqID, queryID=qID, agentID="", connected=False,
                                 connectionSuccessful=False, peerID="", timeTook=-1,
                                 connectionsToPeer=[])
                        dials[dID] = d
                    if aux[1].__contains__("Are we conne"):
                        aux = aux[1].split(" ? ")
                        d.peerID = aux[0].removeprefix("Are we connected to peer ")
                        d.connected = bool(aux[1])
                    elif aux[1].__contains__("Successful conn"):
                        aux = aux[1].split(" took ")
                        d.timeTook = timeTookConvert(aux[2])
                        # TODO falta ligar com a cena anterior
                    elif aux[1].__contains__("protocol:"):
                        aux = aux[1].split("")
                        for x in aux[1:]:
                            conn = x.split("/")
                            if conn[1] == "ip4":
                                d.connectionsToPeer.append(ipaddress.IPv4Address(conn[2]))
                            else:
                                print(conn[1])
                    dials[dID] = d
                else:
                    aux = l.split(" ")  # aux1 ID: .... aux2: <queryID> e mais
                    dID = aux[0].removeprefix("ID: ")
                    if dials.__contains__(dID):
                        d = dials[dID]
                        assert (d.dialID == dID)
                    else:
                        d = Dial(dialID=dID, requestID="", queryID="", agentID="", connected=False,
                                 connectionSuccessful=False, peerID="", timeTook=-1,
                                 connectionsToPeer=[])
                        dials[dID] = d
                    if aux[1].__contains__("Are we conne"):
                        aux = aux[1].split(" ? ")
                        d.peerID = aux[0].removeprefix("Are we connected to peer ")
                        d.connected = bool(aux[1])
                    elif aux[1].__contains__("Successful conn"):
                        aux = aux[1].split(" took ")
                        d.timeTook = timeTookConvert(aux[2])
                        # TODO falta ligar com a cena anterior
                    elif aux[1].__contains__("protocol:"):
                        aux = aux[1].split("")
                        for x in aux[1:]:
                            conn = x.split("/")
                            if conn[1] == "ip4":
                                d.connectionsToPeer.append(ipaddress.IPv4Address(conn[2]))
                            else:
                                print(conn[1])
                    dials[dID] = d
            elif l.__contains__("query to peer"):
                if l.__contains__("ID: "):
                    if l.__contains__("Starting query"):
                        aux = l.split(" Starting query to peer ")  # aux1 ID: .... aux2: <queryID> e mais
                        reqID = aux[0].removeprefix("ID: ")
                        aux = aux[1].split(" for the key ")
                        peerID = aux[0]  # <queryID> // <dialID>..
                        aux = aux[1].split(" encoded ")
                        aux = aux[1].split(" query ID is ")
                        key = aux[0]
                        qID = aux[1]
                        if queries.__contains__(qID):
                            q = queries[qID]
                            assert (q.requestID == reqID)
                            assert (q.queryID == qID)
                        else:
                            q = Query(requestID=reqID, queryID=qID, agentID="", peerID=peerID, timeTook=-1, key=key,
                                      timeStarted=None, timeFinished=None)
                            queries[qID] = q
                    else:
                        q = None
                        aux = l.split(" query ID: ")  # aux1 ID: .... aux2: <queryID> e mais
                        reqID = aux[0].removeprefix("ID: ")
                        aux = aux[1].split(" ")
                        qID = aux[0]  # <queryID> // <dialID>..
                        if queries.__contains__(qID):
                            q = queries[qID]
                            assert (q.requestID == reqID)
                            assert (q.queryID == qID)
                        else:
                            q = Query(requestID=reqID, queryID=qID, agentID="", peerID="", timeTook=-1, key="",
                                      timeStarted=None, timeFinished=None)
                            queries[qID] = q
                        if aux[1].__contains__("Started que"):
                            aux = aux[1].split(" at time ")
                            q.peerID = aux[0].removeprefix("Started query to peer ")
                            q.timeStarted = timestampConvert(aux[1])
                        elif aux[1].__contains__("Finished quer"):
                            aux = aux[1].split(" at time ")
                            q.timeFinished = timestampConvert(aux[1])
                            aux = aux[0].split(" took ")
                            q.timeTook = timeTookConvert(aux[1])
                            # assert (q.peerID == aux[0].removeprefix("Finished query to peer ")
                        queries[qID] = q
            elif l.__contains__("Contacted peer"):
                if l.__contains__("ID: "):
                    aux = l.split(" Contacted peer ")
                    reqID = aux[0].removeprefix("ID: ")
                    aux = aux[1].split(" to get key ")
                    peerID = aux[0]
                    aux = aux[1].split(" peer ID from key is ")
                    aux = aux[1].split(" received ")
                    peerTarget = aux[0]
                    noPeersReceived = int(aux[1])
                    g = GetClosestPeersLookup(requestID=reqID, agentID="", peerContacted=peerID,
                                              peerTargeted=peerTarget,
                                              nPeersReceived=noPeersReceived)
                    getClosestPeersLookup.append(g)
                else:
                    aux = l.split(" to get closest peers to key  ")
                    peerID = aux[0]
                    aux = aux[1].split(" peer ID from key is ")
                    aux = aux[1].split(" received ")
                    peerTarget = aux[0]
                    noPeersReceived = int(aux[1])
                    g = GetClosestPeersLookup(requestID="", agentID="", peerContacted=peerID, peerTargeted=peerTarget,
                                              nPeersReceived=noPeersReceived)
                    getClosestPeersLookup.append(g)
            elif l.__contains__("Sent getClosestPeers"):
                aux = l.split(" at time ")
                pID = aux[0].removeprefix("Sent getProviders message to ")
                aux = aux[1].split(" for key ")
                timeStart = timestampConvert(aux[0])
                aux = aux[1].split(" request ID is ")
                peerTarget = aux[0]
                aux = aux[1].split(" message uid is ")
                reqID = aux[0]
                mesID = aux[1]
                g = GetClosestPeers(requestID=reqID, messageID=mesID, agentID=""  # TODO\
                                    , peerTargeted=peerTarget, timeStarted=timeStart, timeFinished=None, timeTook=None,
                                    peerContacted=pID, nPeersReceived=-1)
                getClosestPeers[mesID] = g
            elif l.__contains__("Received response to getClosest"):
                aux = l.split(" took ")
                aux = aux[1].split(" at time ")
                tTook = timeTookConvert(aux[0])
                aux = aux[1].split(" request ID is ")
                timeFinish = timestampConvert(aux[0])
                aux = aux[1].split(" message uid is ")
                reqID = aux[0]
                mesID = aux[1]
                g = getClosestPeers[mesID]
                g.timeFinished = timeFinish
                g.timeTook = tTook
                getClosestPeers[mesID] = g
            elif l.__contains__("Sent getProviders"):
                aux = l.split(" at time ")
                pID = aux[0].removeprefix("Sent getProviders message to ")
                aux = aux[1].split(" for key ")
                timeStart = timestampConvert(aux[0])
                aux = aux[1].split(" request ID is ")
                keyTarget = aux[0]
                aux = aux[1].split(" message uid is ")
                reqID = aux[0]
                mesID = aux[1]
                gp = GetProviders(requestID=reqID, messageID=mesID, peerID=pID, agentID="", key=keyTarget,
                                  timeStarted=timeStart, timeFinished=None, timeTook=None)
                getProvs[mesID] = gp
            elif l.__contains__("Received response to getProviders"):
                aux = l.split(" took ")
                aux = aux[1].split(" at time ")
                tTook = timeTookConvert(aux[0])
                aux = aux[1].split(" request ID is ")
                timeFinish = timestampConvert(aux[0])
                aux = aux[1].split(" message uid is ")
                reqID = aux[0]
                mesID = aux[1]
                gp = getProvs[mesID]
                gp.timeFinished = timeFinish
                gp.timeTook = tTook
                getProvs[mesID] = gp
                # TODO PutProvider, findProvidersAsyncRoutineMod, runLookupWithFollowUpMod




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    process()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/