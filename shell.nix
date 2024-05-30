let
  nixpkgs-src = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/tarball/nixos-23.05";
  };

  pkgs = import nixpkgs-src { };

  myPython = pkgs.python311;

  shell = pkgs.mkShell {

    shellHook = ''
      # Allow the use of wheels.
      SOURCE_DATE_EPOCH=$(date +%s)
      VENV_PATH=/home/$USER/.venvs$(pwd)/venv${myPython.version}
      # Augment the dynamic linker path

      # Setup the virtual environment if it doesn't already exist.
      if test ! -d $VENV_PATH; then
        python -m venv $VENV_PATH
      fi
      if test -e requirements.txt; then
        $VENV_PATH/bin/pip install -U -r requirements.txt
      fi
      $VENV_PATH/bin/pip install build twine
      source $VENV_PATH/bin/activate
      export PYTHONPATH=$VENV_PATH/${myPython.sitePackages}/:$PYTHONPATH      
    '';
  };
in

shell