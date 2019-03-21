package qcore

type Consumer interface {
    UnPause()
    Pause()
    Close() error
    TimedOutMessage()
    //Stats() ClientStats
    Empty()
}