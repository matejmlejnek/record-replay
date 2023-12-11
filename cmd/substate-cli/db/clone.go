package db

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/research"
	cli "github.com/urfave/cli/v2"
)

var CloneCommand = &cli.Command{
	Action: clone,
	Name:   "db-clone",
	Usage:  "Create a clone DB of a given block segment",
	Flags: []cli.Flag{
		research.WorkersFlag,
		research.BlockSegmentFlag,
		&cli.PathFlag{
			Name:     "src-path",
			Usage:    "Source DB path",
			Required: true,
		},
		&cli.PathFlag{
			Name:     "dst-path",
			Usage:    "Destination DB path",
			Required: true,
		},
	},
	Description: `
substate-cli db clone creates a clone DB of a given block segment.
This loads a complete substate from src-path, then save it to dst path.
The dst-path will always store substates in the latest encoding.
`,
	Category: "db",
}

type HashAndAddress struct {
	Address common.Address
	Hash    common.Hash
}

func clone(ctx *cli.Context) error {
	var err error

	srcPath := ctx.Path("src-path")
	srcBackend, err := rawdb.NewLevelDBDatabase(srcPath, 1024, 100, "srcDB", true)
	if err != nil {
		return fmt.Errorf("substate-cli db clone: error opening %s: %v", srcPath, err)
	}
	srcDB := research.NewSubstateDB(srcBackend)
	defer srcDB.Close()

	// Create dst DB
	dstPath := ctx.Path("dst-path")
	dstBackend, err := rawdb.NewLevelDBDatabase(dstPath, 1024, 100, "srcDB", false)
	if err != nil {
		return fmt.Errorf("substate-cli db clone: error creating %s: %v", dstPath, err)
	}
	dstDB := research.NewSubstateDB(dstBackend)
	defer dstDB.Close()

	fixMap := make(map[string]HashAndAddress)
	fillFixMap(&fixMap)

	cloneTask := func(block uint64, tx int, substate *research.Substate, taskPool *research.SubstateTaskPool) error {
		//check FixMap
		if addressAndHash, ok := fixMap[fmt.Sprintf("%v;%v", block, tx)]; ok {
			ssIn := substate.InputAlloc[addressAndHash.Address]
			delete(ssIn.Storage, addressAndHash.Hash)
			substate.InputAlloc[addressAndHash.Address] = ssIn

			ssOut := substate.OutputAlloc[addressAndHash.Address]
			delete(ssOut.Storage, addressAndHash.Hash)
			substate.OutputAlloc[addressAndHash.Address] = ssOut
		}
		dstDB.PutSubstate(block, tx, substate)

		return nil
	}

	taskPool := &research.SubstateTaskPool{
		Name:     "substate-cli db clone",
		TaskFunc: cloneTask,
		Config:   research.NewSubstateTaskConfigCli(ctx),

		DB: srcDB,
	}

	segment, err := research.ParseBlockSegment(ctx.String(research.BlockSegmentFlag.Name))
	if err != nil {
		return fmt.Errorf("substate-cli db clone: error parsing block segment: %s", err)
	}

	err = taskPool.ExecuteSegment(segment)

	return err
}

func fillFixMap(fixMap *map[string]HashAndAddress) {
	(*fixMap)["12523175;160"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523179;63"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523179;64"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523180;91"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523180;92"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523179;65"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523180;94"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523179;62"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523180;93"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523186;70"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523185;81"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523186;69"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523186;72"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523187;36"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523185;79"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523186;68"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523187;20"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523185;80"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523187;42"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523186;71"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523187;38"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523204;129"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523217;84"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523217;86"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523219;150"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523219;149"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523220;51"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523220;52"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523220;50"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523220;54"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523235;143"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523235;142"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523236;106"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523236;107"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523236;104"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523236;105"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523235;144"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523237;110"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523269;105"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523269;108"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523270;64"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523270;67"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523270;65"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523269;107"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523270;66"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523270;63"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523273;95"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523273;96"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523273;94"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523273;93"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523273;97"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523275;20"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523274;144"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523274;161"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523275;19"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
	(*fixMap)["12523275;18"] = HashAndAddress{
		common.HexToAddress("0x30b5264f24D156c3Ec29410189eE05f3a1c29e1e"),
		common.HexToHash("0x746dc7f6cd650ac2827ea324c3193d4d064245937889b2cdaa86018afc8268a1"),
	}
}
