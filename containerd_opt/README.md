# Current thinking

Solution for accelerating the image layers extracting (still testing...):
    
    import/pull -> fetch -> unpack -> apply   


	We add layer.Digest (contentDigest) to meta.db as a key like "contentDigest": 2719033213..asdffff.
	ChainID is still used to sn.prepare (snapshot dir creating), but before layer extracting,
	a compare between layer.Digest will be performed. Once finding the same digest snapshot in
	storage, we use hard links to avoid meaningless decompressing (happening in default containerd).

	In this manner, we can achieve :

	(1) Chaotic, asynchronous and parallel extracting: we can start the unpacking process for each layer
		already on the edge cloud (both cache layers and downloading layers) with the help of Booster's
		imagelize() function. Once all requested layers have been import, image metadata will be fixed.

	(2) We reduce the redundency in snapshot storage while saving the decompressing time.
