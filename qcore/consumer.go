package qcore

type Consumer interface {
	UnPause()
	Pause()
	Close()
	TimedOutMessage()
	//Stats() ClientStats
	Empty()
}
