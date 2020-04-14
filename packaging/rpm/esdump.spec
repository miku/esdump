Summary:    Stream documents from elasticsearch to stdout.
Name:       esdump
Version:    0.1.5
Release:    0
License:    GPL
ExclusiveArch:  x86_64
BuildRoot:  %{_tmppath}/%{name}-build
Group:      System/Base
Vendor:     Archiving Tools
URL:        https://git.archive.org/martin/esdump

%description

Stream documents from elasticsearch to stdout.

%prep

%build

%pre

%install

mkdir -p $RPM_BUILD_ROOT/usr/local/bin
install -m 755 esdump $RPM_BUILD_ROOT/usr/local/bin

%post

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf %{_tmppath}/%{name}
rm -rf %{_topdir}/BUILD/%{name}

%files
%defattr(-,root,root)

/usr/local/bin/esdump

%changelog

* Thu Apr 9 2020 Martin Czygan
- 0.1.0 release
