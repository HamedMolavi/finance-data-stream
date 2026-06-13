This is a provider package. It works through web socket connections to the Trading View server.

Sockets could be un-authorized but in order to have full functionality they need to be authorized.

Each socket can stream multiple symbol-interval data through chart sessions. We create a chart session via a sequence of messages indicating an ID, the symbols and intervals that session is responsible for and some session specific data like how many earlier finished candles should be sent first.

Then in reading-categorizing loop, when we recognize a new series is formed we find out its chart session and route regarding data to the pipeline we handed to that chart session.